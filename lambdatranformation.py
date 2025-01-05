import json
import boto3
import uuid
import logging
import csv
import urllib.parse
from io import StringIO

# Initialize boto3 clients
s3_client = boto3.client('s3')

# Define the target S3 bucket name for transformed data
S3_TARGET_BUCKET = 'spotify-transformed-data-umer'

# Lambda function handler
def lambda_handler(event, context):
    try:
        # Extract the S3 bucket name and object key from the event
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        source_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        
        # Download the raw data from the source S3 bucket
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        raw_data = response['Body'].read().decode('utf-8')
        
        # Check if the raw data is a valid JSON string (this ensures it's a dictionary/list)
        try:
            raw_data = json.loads(raw_data)
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Failed to decode JSON data.',
                    'error': str(e)
                })
            }
        
        # Now raw_data should be a Python dictionary or list, proceed with transformation
        transformed_data = []
        for album in raw_data:
            # Check if album is a dictionary before proceeding
            if isinstance(album, dict):
                album_info = {
                    'album_id': album.get('id'),
                    'name': album.get('name'),
                    'release_date': album.get('release_date'),
                    'artists': ";".join([artist.get('name') for artist in album.get('artists', [])]),
                    'image_urls': ";".join([image.get('url') for image in album.get('images', [])]),
                    'spotify_url': album.get('external_urls', {}).get('spotify'),
                    'total_tracks': album.get('total_tracks')
                }
                transformed_data.append(album_info)
            else:
                logging.warning(f"Skipping invalid album data: {album}")
        
        # Generate a unique file name for the transformed data (new object in target S3 bucket)
        transformed_s3_key = f"transformed_data/spotify_data_{uuid.uuid4().hex}.csv"
        
        # Write the transformed data to a CSV format
        csv_buffer = StringIO()
        csv_writer = csv.DictWriter(
            csv_buffer, 
            fieldnames=['album_id', 'name', 'release_date', 'artists', 'image_urls', 'spotify_url', 'total_tracks']
        )
        csv_writer.writeheader()
        csv_writer.writerows(transformed_data)
        
        # Upload the transformed data as a CSV file to the target S3 bucket
        s3_client.put_object(
            Bucket=S3_TARGET_BUCKET,
            Key=transformed_s3_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data transformed and uploaded successfully!',
                'transformed_s3_key': transformed_s3_key
            })
        }
    
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process the data',
                'error': str(e)
            
            })
        }