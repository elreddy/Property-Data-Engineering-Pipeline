# hoa_load_lookups.py

import logging
from db import get_connection
from extraction import extract_json

def load_hoa_lookup(file_path):
    """
    Loads unique HOA lookup values from a JSON file and inserts them into the hoa_lookup table.
    """
    try:
        # Extract data from JSON file
        data = extract_json(file_path)
        logging.info(f"Loaded data from {file_path} for HOA lookup extraction.")

        # Establish database connection
        conn = get_connection()
        if conn is None:
            logging.error("Database connection is None.")
            print("Error: Could not connect to the database.")
            return
        cursor = conn.cursor()
        logging.info("Database connection established.")

        unique_hoa_records = set()

        # Collect unique (hoa_value, hoa_flag) pairs from the data
        for row in data:
            hoa_list = row.get("HOA", [])
            for hoa_entry in hoa_list:
                hoa_value = hoa_entry.get("HOA")
                hoa_flag = hoa_entry.get("HOA_Flag")
                if hoa_value is not None and hoa_flag is not None:
                    unique_hoa_records.add((hoa_value, hoa_flag))
        logging.info(f"Extracted {len(unique_hoa_records)} unique HOA lookup records.")

        # Insert unique HOA lookup records into the database
        for value, flag in unique_hoa_records:
            try:
                cursor.execute(
                    "INSERT IGNORE INTO hoa_lookup (hoa_value, hoa_flag) VALUES (%s, %s)",
                    (value, flag)
                )
                logging.debug(f"Inserted HOA lookup ({value}, {flag})")
            except Exception as e:
                logging.error(f"Failed to insert HOA lookup ({value}, {flag}): {e}")
                print(f"Error inserting HOA lookup ({value}, {flag}). Check logs for details.")

        logging.info(f"Inserted {len(unique_hoa_records)} unique HOA records into hoa_lookup.") 
        conn.commit()
        logging.info("Database commit successful for HOA lookups.")
        cursor.close()
        conn.close()
        logging.info("Database connection closed after HOA lookup load.")
    except Exception as e:
        logging.error(f"Failed to load HOA lookups: {e}")
        print("Error: Could not load HOA lookups. Check logs for details.")

