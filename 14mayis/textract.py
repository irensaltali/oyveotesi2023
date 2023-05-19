import json
import os
import requests
import boto3
import time
import re
# Configure AWS credentials and region for Textract
AWS_REGION = 'eu-central-1'
S3_BUCKET = 'xx'

def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        # create new row
                        rows[row_index] = {}
                        
                    # get the text value
                    rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows

def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] =='SELECTED':
                            text +=  'X '    
    return text

def generate_table_csv(table_result, blocks_map, table_index):
    rows = get_rows_columns_map(table_result, blocks_map)

    table_id = 'Table_' + str(table_index)
    
    # get cells.
    csv = 'Table: {0}\n\n'.format(table_id)

    for row_index, cols in rows.items():
        
        for col_index, text in cols.items():
            csv += '{}'.format(text) + ","
        csv += '\n'
        
    csv += '\n\n\n'
    return csv

def get_vote_count(data):
    # Define the candidate names to extract vote counts for
    candidates = ["RECEP TAYYIP ERDOGAN", "MUHARREM INCE", "KEMAL KILICDAROGLU", "SINAN OGAN"]

    # Initialize an empty dictionary to store the vote counts
    vote_counts = {}

    # Extract the numbers for each candidate
    for candidate in candidates:
        # Use regular expressions to find the numbers after the candidate's name
        pattern = re.compile(f"{candidate}\s*,\s*(\d+)")
        match = pattern.search(data)

        if match:
            # Extract the vote count
            vote_count = int(match.group(1))
            # Store the vote count in the dictionary
            vote_counts[candidate] = vote_count
    
    # Calculate the sum of vote counts
    sum_votes = sum(vote_counts.values())

    # Extract the total vote count
    pattern = re.compile("TOPLAM\s*,\s*(\d+)")
    match = pattern.search(data)

    if match:
        total_votes = int(match.group(1))
        # Store the total vote count in the dictionary
        vote_counts["TOPLAM"] = total_votes

    if sum_votes != total_votes:
        print("##### Sum of vote counts does not match the total vote count.")

    # Print the vote counts dictionary
    print(vote_counts)

def get_vote_count2(data):
    # Define the candidate names to extract vote counts for
    candidates = ["RECEP TAYYIP ERDOGAN", "MUHARREM INCE", "KEMAL KILICDAROGLU", "SINAN OGAN"]

    # Initialize an empty dictionary to store the vote counts
    vote_counts = {}

    # Tokenize the data
    data_tokens = data.split()

    # Iterate over the tokens in the data
    for token in data_tokens:
        # Check if the token is a fuzzy match for any candidate
        for candidate in candidates:
            if fuzz.ratio(token.upper(), candidate) > 40:  # 80 is a threshold you can adjust
                # If it is a match, find the vote count that follows the candidate's name
                pattern = re.compile(f"{token}\s*,\s*(\d+)")
                match = pattern.search(data)

                if match:
                    # Extract the vote count
                    vote_count = int(match.group(1))
                    # Store the vote count in the dictionary
                    vote_counts[candidate] = vote_count
                    break  # Break the inner loop once we found a match
    
    # Calculate the sum of vote counts
    sum_votes = sum(vote_counts.values())

    # Extract the total vote count
    pattern = re.compile("TOPLAM\s*,\s*(\d+)")
    match = pattern.search(data)

    if match:
        total_votes = int(match.group(1))
        # Store the total vote count in the dictionary
        vote_counts["TOPLAM"] = total_votes

    if sum_votes != total_votes:
        print("##### Sum of vote counts does not match the total vote count.")

    # Print the vote counts dictionary
    print(vote_counts)
    
def main():
    # Create directories for images and Textract data
    os.makedirs('images', exist_ok=True)
    os.makedirs('textract', exist_ok=True)

    # Create a session for AWS Textract and S3
    session = boto3.Session(
        profile_name='irensaltali',
        region_name=AWS_REGION
    )
    textract_client = session.client('textract')

    # Iterate over each ballot box data file
    for filename in os.listdir('ballot_box'):
        print(f'Processing {filename}...')
        ballot_box_id = os.path.splitext(filename)[0]
        file_path = f'ballot_box/{filename}'

        with open(file_path) as file:
            ballot_box_data = json.load(file)

        for ballot_box_item in ballot_box_data:
            cm_result = ballot_box_item.get('cm_result')

            # Skip if cm_result is None
            if cm_result is None:
                print(f'Skipping ballot box {ballot_box_id}: cm_result is None')
                continue

            image_url = cm_result.get('image_url', '')

            # Skip if image_url is empty
            if not image_url:
                print(f'Skipping ballot box {ballot_box_id}: image_url is empty')
                continue
            else:
                # Check if image already exists in local folder before uploading
                local_image_path = os.path.join('.', f'images/{ballot_box_id}/cm.jpg')
                if os.path.exists(local_image_path):
                    print(f'Image already exists in local folder for ballot box {ballot_box_id}')
                else:
                    # Image doesn't exist in local folder, proceed with download and save
                    response = requests.get(image_url)
                    if response.status_code == 200:
                        os.makedirs(os.path.dirname(local_image_path), exist_ok=True)
                        with open(local_image_path, 'wb') as image_file:
                            image_file.write(response.content)
                        print(f'Saved image for ballot box {ballot_box_id} in local folder')
                    else:
                        print(f'Error downloading image for ballot box {ballot_box_id}')
                        continue

            # Check if AWS Textract data already exists before sending to AWS
            textract_data_path = f'textract/{ballot_box_id}/textract_data.json'
            if os.path.exists(textract_data_path) is False:
                # Call AWS Textract to process the image
                with open(local_image_path, 'rb') as image_file:
                    image_data = image_file.read()

                response = textract_client.analyze_document(
                    Document={'Bytes': image_data},
                    FeatureTypes=[
                        'TABLES',
                    ])
                
                # Save the Textract response as JSON
                os.makedirs(os.path.dirname(textract_data_path), exist_ok=True)
                with open(textract_data_path, 'w') as textract_file:
                    json.dump(response, textract_file)
                
                blocks=response['Blocks']
                blocks_map = {}
                table_blocks = []
                for block in blocks:
                    blocks_map[block['Id']] = block
                    if block['BlockType'] == "TABLE":
                        table_blocks.append(block)

                if len(table_blocks) <= 0:
                    print("<b> NO Table FOUND </b>")

                csv = ''
                for index, table in enumerate(table_blocks):
                    csv += generate_table_csv(table, blocks_map, index +1)

                csv = re.sub(r'[^A-Za-z0-9, ]', '', csv)

                textract_table_path = f'textract/{ballot_box_id}/textract_table.csv'
                with open(textract_table_path, 'w') as textract_table_file:
                    textract_table_file.write(csv)

                get_vote_count(csv)

                # print(f'Saved AWS Textract data for ballot box {ballot_box_id}')

                print('\n\n\n')

if __name__ == "__main__":
    main()
