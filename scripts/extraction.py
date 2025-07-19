# Import necessary libraries
import json

# Function to extract data from a JSON file and return it as a list of dictionaries
def extract_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

