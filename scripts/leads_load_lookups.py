import logging
from db import get_connection
from extraction import extract_json


LOOKUPS = {
    "Source": ("source_lookup", "source_name"),
    "Selling_Reason": ("selling_reason_lookup", "selling_reason"),
    "Final_Reviewer": ("final_reviewer_lookup", "reviewer_name"),
}

def load_leads_lookups(file_path):
    try:
        data = extract_json(file_path)
        conn = get_connection()
        if conn is None:
            logging.error("Database connection is None.")
            print("Error: Could not connect to the database.")
            return
        cursor = conn.cursor()

        for field, (table, column) in LOOKUPS.items():
            unique_values = set()
            for record in data:
                value = record.get(field.capitalize()) or record.get(field)
                if value:
                    unique_values.add(value.strip())

            for value in unique_values:
                try:
                    cursor.execute(f"INSERT IGNORE INTO {table} ({column}) VALUES (%s)", (value,))
                except Exception as e:
                    logging.error(f"Error inserting into {table} ({value}): {e}")
                    print(f"Error inserting into {table}: {e}")
                    
            logging.info(f"Loaded {len(unique_values)} unique values into {table}.")
            conn.commit()

        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Failed to load leads lookups: {e}")
        print("Error: Could not load leads lookups.")
