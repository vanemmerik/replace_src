import csv
import os
import requests
import json
import time
import base64
import subprocess
import re
from datetime import datetime

# Brightcove API Credentials
client_id = 'YOUR CLIENT ID HERE'
client_secret = 'YOUR CLIENT SECRET HERE'
account_id = 'PUB ID HERE'

# AWS CLI settings
url_expiry = '1800' # URL expiry set - 30 minutes 
aws_region = 'ap-southeast-2' # AWS region 
aws_cli_profile = 'YOUR AWS CLI PROFILE HERE' # Add your AWS CLI credential profile here

# Regex to check URL prefix and file type for ingestion
vid_url_pattern = r'^(https?://|s3://)[^/]+/(?:.+/)?[^/]+(?:\.(mp4|mov|avi|mkv))$'
# Timestamp used for logfile
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def generate_signed_url(video_url):
    try:
        cmd = f"aws s3 presign '{video_url}' --profile {aws_cli_profile} --expires-in {url_expiry} --region {aws_region}"
        
        result = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, universal_newlines=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        return None

def is_valid_video_url(video_url):
    # Check if video_url is a string and perform validations
    if isinstance(video_url, str):
        if re.match(vid_url_pattern, video_url):
            return True, None
        else:
            return False, "Provided URL is not a valid URL path or video format"
    else:
        return False, "URL is not a string or is missing"

# CSV and other file paths for logging

csv_path = 'ingest.csv' # Define the path to your CSV here for ingest processing
failure_log_file = f'logs/failed_video_urls_{current_time}.txt'
last_processed_id_file = 'scratch/last_processed_id.txt'

# Brightcove OAuth URL
oauth_url = 'https://oauth.brightcove.com/v4/access_token'

# Brightcove Ingest API endpoint template
ingest_api_template = 'https://ingest.api.brightcove.com/v1/accounts/{}/videos/{}/ingest-requests'

video_info_api_template = 'https://cms.api.brightcove.com/v1/accounts/{}/videos/{}'

# Global variable to store the access token and expiry time
token_info = {'access_token': None, 'expires_in': None, 'acquired_at': None}

# Rate Limiting Setup
request_limit = 150  # Max number of requests
time_frame = 60  # Time frame in seconds
request_interval = time_frame / request_limit  # Interval between requests in seconds

# Function to get or refresh the OAuth token with Base64 encoding
def get_or_refresh_token():
    global token_info
    current_time = time.time()
    
    # Check if the token is still valid
    if token_info['access_token'] and (current_time - token_info['acquired_at']) < token_info['expires_in']:
        return token_info['access_token']
    else:
        print("Requesting new OAuth token...")
        # Encode client_id and client_secret in Base64
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {encoded_credentials}'
        }
        
        response = requests.post(oauth_url, headers=headers, data='grant_type=client_credentials')
        
        if response.status_code == 200:
            token_data = response.json()
            token_info = {
                'access_token': token_data['access_token'],
                'expires_in': token_data.get('expires_in', 300) - 30,  # Subtract a buffer
                'acquired_at': current_time
            }
            print("New OAuth token acquired.")
            return token_info['access_token']
        else:
            print("Failed to get OAuth token:", response.text)
            return None

# Function to check if the video_id exists
def video_exists_brightcove(video_id, reader):
    
    if not video_id.isdigit():
        print(f"Video ID: {video_id} - Not valid format.")
        with open(failure_log_file, 'a') as log:
            log.write(f"Row: {reader}, Video ID: {video_id}, Video URL: N/A, Reason: Video ID not a valid format.\n")
        return False
    
    access_token = get_or_refresh_token()
    
    if not access_token:
        return

    video_info_url = video_info_api_template.format(account_id, video_id)
    info_response = requests.get(video_info_url, headers={
        'Authorization': f'Bearer {access_token}',
    })

    json_response = json.loads(info_response.text)

    if 'name' in json_response:
        print(f"Video {json_response['name']} exists.")
        return True
    else:
        print(f"Video ID: {video_id} - {json_response[0]['error_code']} - {json_response[0]['message']}")
        with open(failure_log_file, 'a') as log:
            log.write(f"Row: {reader}, Video ID: {video_id}, Video URL: N/A, Reason: CMS API response message - {json_response[0]['error_code']} - {json_response[0]['message']}\n")
        return False

# Function to send video data to Brightcove Ingest API
def send_to_brightcove(video_id, video_url):
    access_token = get_or_refresh_token()
    if not access_token:
        return

    api_url = ingest_api_template.format(account_id, video_id)
    response = requests.post(api_url, headers={
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }, json={
        'master': {'url': video_url},
        'profile': 'multi-platform-standard-static',  # Replace with specified ingest profile if needed
        'priority': 'low',
        'capture-images': False
    })

    if response.status_code in [200, 202]:
        print(f"Successfully ingested video ID {video_id}.")
    else:
        json_response = json.loads(response.text)
        print(f"Failed to ingest video ID {video_id}: {json_response[0]['error_code']}.")
        with open(failure_log_file, 'a') as log:
            log.write(f"Video ID: {video_id}, Video URL: {video_url}, Reason: API response message - {json_response[0]['error_code']}\n")

# Function to save the last processed video ID
def save_last_processed_id(video_id):
    with open(last_processed_id_file, 'w') as file:
        file.write(video_id)

# Function to get the last processed video ID
def get_last_processed_id():
    if os.path.exists(last_processed_id_file):
        with open(last_processed_id_file, 'r') as file:
            return file.read().strip()
    return None

# Modified main function to resume from last processed ID and announce completion
def main():
    last_processed_id = get_last_processed_id()
    found_last_id = False if last_processed_id else True
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for idx, row in enumerate(reader, 1):
            if not found_last_id:
                if row['video_id'] == last_processed_id:
                    found_last_id = True
                continue

            video_id, video_url, delivery_type = row['video_id'], row['video_url'], row['delivery_type']  # Adjust these CSV indices

            valid_url, error_msg = is_valid_video_url(video_url)
            if (delivery_type) != 'remote' or not valid_url:
                # print(f"error_msg: {error_msg}")  # Handle the error accordingly
                error_reason = f"Invalid delivery type: {delivery_type}" if (delivery_type) != 'remote' else error_msg
                if not delivery_type.strip():
                    error_reason = f"Invalid delivery type: undefined"
                print(f"Skipping video_id: {video_id} due to - {error_reason}.")
                with open(failure_log_file, 'a') as log:
                    log.write(f"Row: {reader.line_num}, Video ID: {video_id}, Video URL: {video_url}, Reason: {error_reason}\n")
                continue    
            
            if not video_exists_brightcove(video_id, reader.line_num):
                continue

            if idx and idx % request_limit == 0:
                print("Rate limit reached, pausing...")
                time.sleep(time_frame)  # Pause script to respect rate limit

            signed_url = generate_signed_url(video_url)

            send_to_brightcove(video_id, signed_url)
            time.sleep(request_interval)  # Wait for the calculated interval between requests
            print(f"Waiting {request_interval} seconds to respect rate limit...")
            
            # After successfully processing a video ID:
            save_last_processed_id(video_id)

    if os.path.exists(last_processed_id_file):
        with open(last_processed_id_file, 'r') as file:
            contents = file.read()
            if contents:
                with open(last_processed_id_file, 'w'):
                    pass
                print(f"Removing last recorded video_id {last_processed_id_file} -- processing is complete.")
            else:
                print(f"{last_processed_id_file} is already empty.")
        print("CSV processing has finished.")

if __name__ == '__main__':
    main()