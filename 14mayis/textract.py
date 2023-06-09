import json
import os
import requests
import boto3
import time
import re
from google.cloud import vision
from google.oauth2 import service_account

# Configure AWS credentials and region for Textract
AWS_REGION = 'eu-central-1'
S3_BUCKET = 'xx'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentials.json'

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
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '
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


def get_vote_count_from_textract(data):
    # Define the candidate names to extract vote counts for
    candidates = {
        "RECEP TAYYIP ERDOGAN": ["RECEP TAYYIP ERDOGAN", "RECEP TAYYP ERDOGAN", "RECEP TAYYIP ERD0GAN", "RECEP TAYYP ERD0GAN", "RECEP TAYYIP ERD0GAN"],
        "MUHARREM INCE": ["MUHARREM INCE", "MUHARREM 1NCE", "MUHAREM INCE", "MUHARREM INÇE"],
        "KEMAL KILICDAROGLU": ["KEMAL KILICDAROGLU", "KEMAL KILICOAROGLU", "KEMAL K1LICDAROGLU", "KEMA_ KILICDAROGLU", "KEMAL KILICDAR0GLU", "KEMA_ K1LICDAR0GLU"],
        "SINAN OGAN": ["SINAN OGAN", "SINAN 0GAN", "S1NAN OGAN", "SINAN OG_N", "S1NAN 0G_N", "SNAN 0GAN"]
    }

    # Initialize an empty dictionary to store the vote counts
    vote_counts = {}

    # Extract the numbers for each candidate and their typos
    for candidate, typos in candidates.items():
        for typo in typos:
            # Use regular expressions to find the numbers after the candidate's name or typo
            pattern = re.compile(f"{typo}\s*,\s*(\d+)")
            match = pattern.search(data)

            if match:
                # Extract the vote count
                vote_count = int(match.group(1))
                # If candidate already has a vote count, add this count to the existing value
                if candidate in vote_counts:
                    vote_counts[candidate] += vote_count
                # Otherwise, store the vote count in the dictionary
                else:
                    vote_counts[candidate] = vote_count
                # Since a match is found, proceed to the next candidate/typo
                break

    # Extract the total vote count
    pattern = re.compile("TOPLAM\s*,\s*(\d+)")
    match = pattern.search(data)

    if match:
        total_votes = int(match.group(1))
        # Store the total vote count in the dictionary
        vote_counts["TOPLAM"] = total_votes

    return vote_counts


def textract_to_csv(response):
    blocks = response['Blocks']
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
        csv += generate_table_csv(table, blocks_map, index + 1)

    csv = re.sub(r'[^A-Za-z0-9, ]', '', csv)

    return csv

def main():
    # Create directories for images and Textract data
    os.makedirs('images', exist_ok=True)
    os.makedirs('textract', exist_ok=True)
    os.makedirs('not_same', exist_ok=True)

    # Create a session for AWS Textract and S3
    session = boto3.Session(
        profile_name='irensaltali',
        region_name=AWS_REGION
    )
    textract_client = session.client('textract')

    # Iterate over each ballot box data file
    for filename in os.listdir('ballot_boxes_in_school'):
        print(f'Processing {filename}...')
        file_path = f'ballot_boxes_in_school/{filename}'

        with open(file_path) as file:
            ballot_boxes_in_school_data = json.load(file)

        for ballot_box in ballot_boxes_in_school_data:
            cm_result = ballot_box.get('cm_result')
            ballot_box_number = ballot_box.get('ballot_box_number')
            print(f'Processing ballot box {ballot_box_number}...')

            # Skip if cm_result is None
            if cm_result is None:
                print(
                    f'Skipping ballot box {ballot_box_number}: cm_result is None')
                continue

            image_url = cm_result.get('image_url', '')

            # Skip if image_url is empty
            if not image_url:
                print(
                    f'Skipping ballot box {ballot_box_number}: image_url is empty')
                continue
            else:
                # Check if image already exists in local folder before uploading
                local_image_path = os.path.join(
                    '.', f'images/{ballot_box_number}/cm.jpg')
                if not os.path.exists(local_image_path):
                    # Image doesn't exist in local folder, proceed with download and save
                    response = requests.get(image_url)
                    if response.status_code == 200:
                        os.makedirs(os.path.dirname(local_image_path), exist_ok=True)
                        with open(local_image_path, 'wb') as image_file:
                            image_file.write(response.content)
                        # print(f'Saved image for ballot box {ballot_box_number} in local folder')
                    else:
                        # print(f'Error downloading image for ballot box {ballot_box_number}')
                        continue

            # Check if AWS Textract data already exists before sending to AWS
            textract_data_path = f'textract/{ballot_box_number}/textract_data_cm.json'

            # Check if Textract data exists locally
            if os.path.exists(textract_data_path):
                # Load the Textract data from the local file
                with open(textract_data_path, 'r') as textract_file:
                    response = json.load(textract_file)
            else:
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

            csv = textract_to_csv(response)

            textract_table_path = f'textract/{ballot_box_number}/textract_table_cm.csv'
            with open(textract_table_path, 'w') as textract_table_file:
                textract_table_file.write(csv)

            textract_results = get_vote_count_from_textract(csv)

            total_votes = sum(
                [value for key, value in textract_results.items() if key != "TOPLAM"])

            if total_votes == textract_results["TOPLAM"]:
                print("All candidates' vote count and the total are the same.")
            else:
                print("#################All candidates' vote count and the total are not the same.")
                vision_client = vision.ImageAnnotatorClient()
                
                with open(local_image_path, 'rb') as image_file:
                    image_data = image_file.read()
                image = vision.Image(content=image_data)
                vision_data_path = f'textract/{ballot_box_number}/vision_data_cm.json'
                vision_response = vision_client.document_text_detection(image=image)
                vision_document = vision_response.full_text_annotation
                
                # os.makedirs(os.path.dirname(vision_data_path), exist_ok=True)
                # with open(vision_data_path, 'w') as vision_file:
                #     json.dump(vision_document, vision_file)

                # Process and extract table data
                table_data = []
                for page in vision_document.pages:
                    for block in page.blocks:
                        for paragraph in block.paragraphs:
                            print(paragraph)
                            row_data = []
                            for word in paragraph.words:
                                word_text = ''.join([symbol.text for symbol in word.symbols])
                                row_data.append(word_text)
                            table_data.append(row_data)
                print(table_data)
                # Move the Textract data and image to not_same folder
                not_same_textract_path = f'not_same/{ballot_box_number}/textract_data_cm.json'
                not_same_textract_table_path = f'not_same/{ballot_box_number}/textract_data_table_cm.csv'
                not_same_image_path = f'not_same/{ballot_box_number}/cm.jpg'
                os.makedirs(os.path.dirname(
                    not_same_textract_path), exist_ok=True)
                os.makedirs(os.path.dirname(
                    not_same_textract_table_path), exist_ok=True)
                os.makedirs(os.path.dirname(
                    not_same_image_path), exist_ok=True)

                os.rename(textract_data_path, not_same_textract_path)
                os.rename(textract_table_path, not_same_textract_table_path)
                os.rename(local_image_path, not_same_image_path)

            # print(f'Saved AWS Textract data for ballot box {ballot_box_number}')

            print('\n\n\n')


if __name__ == "__main__":
    main()
