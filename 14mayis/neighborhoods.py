import json
import os
import requests
import time

# Read the city JSON data from the file
with open('cities.json') as file:
    city_data = json.load(file)

# Create directories to store the district and neighborhood JSON files
os.makedirs('districts', exist_ok=True)
os.makedirs('neighborhoods', exist_ok=True)

# Iterate over each city in the data
for city_item in city_data:
    city_id = city_item['id']
    if city_id == 82: # Skip the "Yurtdisi" neighborhood
                continue
    districts_filename = f'./districts/{city_id}.json'

    # Check if the districts JSON file exists
    if not os.path.exists(districts_filename):
        print(f'Districts file not found for city ID {city_id}')
        continue

    # Read the districts JSON data
    with open(districts_filename) as districts_file:
        districts_data = json.load(districts_file)

    # Create a directory for the city's neighborhoods
    city_neighborhoods_dir = f'./neighborhoods/{city_id}'
    os.makedirs(city_neighborhoods_dir, exist_ok=True)

    # Iterate over each district in the city
    for district_item in districts_data:
        district_id = district_item['id']
        neighborhoods_filename = f'{city_neighborhoods_dir}/{district_id}.json'

        # Check if the neighborhoods JSON file already exists
        if os.path.exists(neighborhoods_filename):
            print(
                f'Neighborhoods file already exists for city ID {city_id}, district ID {district_id}')
            continue

        url = f'https://api-sonuc.oyveotesi.org/api/v1/cities/{city_id}/districts/{district_id}/neighborhoods'

        # Add a delay before sending the request
        time.sleep(0.3)

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
        }

        # Send a GET request to the API
        response = requests.get(url, headers=headers)

        # Save the response as JSON
        if response.status_code == 200:
            neighborhood_data = response.json()

            with open(neighborhoods_filename, 'w') as outfile:
                json.dump(neighborhood_data, outfile)

            print(
                f'Saved neighborhoods for city ID {city_id}, district ID {district_id} to {neighborhoods_filename}')
        else:
            print(
                f'Error retrieving neighborhoods for city ID {city_id}, district ID {district_id}: {response.status_code}')
