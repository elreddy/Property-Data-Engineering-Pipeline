# Import necessary libraries
import json
import logging

# Function to extract data from a JSON file and return it as a list of dictionaries
def extract_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logging.info(f"Data extracted successfully from {file_path}.")
        return data
    except Exception as e:
        logging.error(f"Failed to extract JSON from {file_path}: {e}")
        print(f"Error: Could not extract data from {file_path}. Check logs for more details.")

