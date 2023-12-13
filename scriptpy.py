import csv
import os
import requests

# Path to your CSV file
csv_file_path = './public/mahmoud.csv'

# Destination directory
destination_directory = './downloads'

# Create the destination directory if it doesn't exist
os.makedirs(destination_directory, exist_ok=True)

# Set a User-Agent header
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

# Read the CSV file
with open(csv_file_path, 'r', newline='', encoding='utf-8-sig') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        image_id = row['ID']
        image_url = row['URL']

        # Check if the URL is not empty
        if image_url != '':
            # Create a filename using the ID and save it to the destination directory
            filename = os.path.join(destination_directory, f"{image_id}.jpg")

            # Download the image using requests with User-Agent header
            try:
                response = requests.get(image_url, stream=True, headers=headers)
                response.raise_for_status()  # Check for HTTP errors
                with open(filename, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=128):
                        file.write(chunk)
                print(f"Done for --> {filename}")
            except requests.exceptions.RequestException as e:
                print(f"Failed to download image with ID --> {image_id} from {image_url}. Error: {e}")
        else:
            print(f"URL is empty for image with ID --> {image_id} Skipping...")
