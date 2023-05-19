import json
import os
import requests
import time
from glob import glob

# Create the directories to store the ballot box JSON files
os.makedirs('ballot_boxes_in_school', exist_ok=True)

# Retrieve the list of school data files recursively
school_files = glob('schools/*/*/*.json')

# Iterate over each school data file
for school_file in school_files:
    with open(school_file) as file:
        school_data = json.load(file)

    # Check if the school data is a list
    if isinstance(school_data, list):
        # Iterate over each school in the list
        for school_item in school_data:
            school_id = school_item['id']
            ballot_boxes_in_school_filename = f'./ballot_boxes_in_school/{school_id}.json'

            # Check if the ballot box JSON file already exists
            if os.path.exists(ballot_boxes_in_school_filename):
                print(
                    f'Ballot box file already exists for school ID {school_id}')
                continue

            url = f'https://api-sonuc.oyveotesi.org/api/v1/submission/school/{school_id}'

            # Add a delay before sending the request
            time.sleep(0.3)

            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
            }

            # Send a GET request to the API
            response = requests.get(url, headers=headers)

            # Save the response as JSON
            if response.status_code == 200:
                ballot_boxes_in_school_data = response.json()

                with open(ballot_boxes_in_school_filename, 'w') as outfile:
                    json.dump(ballot_boxes_in_school_data, outfile)

                print(
                    f'Saved ballot box data for school ID {school_id} to {ballot_boxes_in_school_filename}')
            else:
                print(
                    f'Error retrieving ballot box data for school ID {school_id}: {response.status_code}')
    else:
        print(f'Skipping invalid school data file: {school_file}')
