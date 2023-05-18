import json
import os
import requests
import time

# Read the JSON data from the file
with open('cities.json') as file:
    data = json.load(file)

# Create a directory to store the district JSON files
os.makedirs('districts', exist_ok=True)

# Iterate over each item in the data
for item in data:
    city_id = item['id']
    url = f'https://api-sonuc.oyveotesi.org/api/v1/cities/{city_id}/districts'
    filename = f'./districts/{city_id}.json'

    # Check if the neighborhoods JSON file already exists
    if os.path.exists(filename):
        print(f'Cities file already exists for city ID {city_id}.')
        continue

    # Add a delay before sending the request
    time.sleep(0.1)
    
    # Send a GET request to the API
    response = requests.get(url)

    # Save the response as JSON
    if response.status_code == 200:
        district_data = response.json()

        with open(filename, 'w') as outfile:
            json.dump(district_data, outfile)

        print(f'Saved districts for city ID {city_id} to {filename}')
    else:
        print(f'Error retrieving districts for city ID {city_id}: {response.status_code}')


