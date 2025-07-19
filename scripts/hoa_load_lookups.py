# hoa_load_lookups.py

import logging
from db import get_connection
from extraction import extract_json

def load_hoa_lookup(file_path):
    try:
        data = extract_json(file_path)
        conn = get_connection()
        if conn is None:
            logging.error("Database connection is None.")
            print("Error: Could not connect to the database.")
            return
        cursor = conn.cursor()

        unique_hoa_records = set()

        for row in data:
            hoa_list = row.get("HOA", [])
            for hoa_entry in hoa_list:
                hoa_value = hoa_entry.get("HOA")
                hoa_flag = hoa_entry.get("HOA_Flag")
                if hoa_value is not None and hoa_flag is not None:
                    unique_hoa_records.add((hoa_value, hoa_flag))

        for value, flag in unique_hoa_records:
            try:
                cursor.execute(
                    "INSERT IGNORE INTO hoa_lookup (hoa_value, hoa_flag) VALUES (%s, %s)",
                    (value, flag)
                )
            except Exception as e:
                logging.error(f"Failed to insert HOA lookup ({value}, {flag}): {e}")
                print(f"Error inserting HOA lookup ({value}, {flag}). Check logs for details.")
        logging.info(f"Inserted {len(unique_hoa_records)} unique HOA records into hoa_lookup.") 
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Failed to load HOA lookups: {e}")
        print("Error: Could not load HOA lookups. Check logs for details.")

