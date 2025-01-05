import requests
import boto3
import json
import os
from datetime import datetime

# Spotify API credentials
SPOTIFY_CLIENT_ID = "5795dc005"
SPOTIFY_CLIENT_SECRET = "2e947b4b403"

# AWS S3 bucket details
AWS_REGION = "us-east-1"
S3_BUCKET_NAME = "spotify-raw-data-umer"
S3_RAW_FOLDER = "raw/"  # Folder in the bucket to store raw files

# Get Spotify API token
def get_spotify_token(client_id, client_secret):
    url = "https://accounts.spotify.com/api/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "client_credentials"}

    response = requests.post(url, headers=headers, data=data, auth=(client_id, client_secret))
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()["access_token"]

# Fetch data from Spotify API
def fetch_spotify_data(token, endpoint="search", params=None):
    base_url = "https://api.spotify.com/v1/"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(base_url + endpoint, headers=headers, params=params)
    response.raise_for_status()
    return response.json()



def fetch_spotify_data2(token, endpoint="browse/new-releases", params=None):
    base_url = "https://api.spotify.com/v1/"
    headers = {"Authorization": f"Bearer {token}"}
    
    # If no params are provided, default to fetching new releases
    if params is None:
        params = {"limit": 50}  # Set a limit for the number of results per request
    
    # Initialize a list to hold bulk data
    bulk_data = []
    
    # Fetch data in a loop to handle pagination
    while True:
        response = requests.get(base_url + endpoint, headers=headers, params=params)
        
        response.raise_for_status()

        # Extract the data from the response
        data = response.json()
        
        # Append the items (songs) to the bulk data list
        bulk_data.extend(data.get('albums', {}).get('items', []))
        
        # Check if there's a next page
        next_url = data.get('albums', {}).get('next')
        if next_url:
            params = {"offset": len(bulk_data), "limit": 50}  # Update offset for the next page
        else:
            break  # No more pages, exit the loop

    return bulk_data


# Upload data to S3
def upload_to_s3(data, bucket_name, s3_folder, file_name, aws_region):
    s3_client = boto3.client("s3", region_name=aws_region)
    file_path = os.path.join(s3_folder, file_name)

    # Convert data to JSON string
    json_data = json.dumps(data)

    # Upload the data
    s3_client = boto3.client(
    "s3",
    region_name=aws_region,
    aws_access_key_id="AKIS",
    aws_secret_access_key="2MF7S4yK3"
    )
    s3_client.put_object(
           Bucket=bucket_name,
           Key=file_path,
           Body=json_data,
           ContentType="application/json"
       )

    print(f"Data successfully uploaded to S3: {bucket_name}/{file_path}")

def main():
    try:
        # Get Spotify API token
        token = get_spotify_token(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)

        spotify_data = fetch_spotify_data2(token)
    # Process the bulk data as needed
        # print(f"Fetched {data} songs.")
        # Fetch Spotify data (e.g., search for "Coldplay")
        # spotify_data = fetch_spotify_data2(
        #     token,
        #     endpoint="search",
        #     params={"q": "Coldplay", "type": "artist", "limit": 1}
        # )
        # print(json.dumps(spotify_data, indent=2))
        # Prepare file name with timestamp
        timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"spotify_data_{timestamp}.json"

        # Upload data to S3
        upload_to_s3(
            data=spotify_data,
            bucket_name=S3_BUCKET_NAME,
            s3_folder=S3_RAW_FOLDER,
            file_name=file_name,
            aws_region=AWS_REGION
        )

    except Exception as e:
        print(f"Error: {e}")

# Run the script
if __name__ == "__main__":
    main()
# CLIENT_ID = 5795dc0e1aad4ef3a25736d0a28d2005
# CLIENT_SECRET = 2e947b46863a496c8e79d5dca383b403
# S3_BUCKET_NAME = spotify-raw-data-umer