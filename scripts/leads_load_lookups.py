import logging
from db import get_connection
from extraction import extract_json

# Mapping of JSON fields to their respective lookup tables and columns
LOOKUPS = {
    "Source": ("source_lookup", "source_name"),
    "Selling_Reason": ("selling_reason_lookup", "selling_reason"),
    "Final_Reviewer": ("final_reviewer_lookup", "reviewer_name"),
}

def load_leads_lookups(file_path):
    """
    Loads unique lookup values for leads (source, selling reason, reviewer) from a JSON file
    and inserts them into their respective lookup tables.
    """
    try:
        # Extract data from JSON file
        data = extract_json(file_path)
        logging.info(f"Loaded data from {file_path} for leads lookup extraction.")

        # Establish database connection
        conn = get_connection()
        if conn is None:
            logging.error("Database connection is None.")
            print("Error: Could not connect to the database.")
            return
        cursor = conn.cursor()
        logging.info("Database connection established.")

        # Iterate over each lookup field and insert unique values
        for field, (table, column) in LOOKUPS.items():
            unique_values = set()
            # Collect unique values for the current field
            for record in data:
                value = record.get(field.capitalize()) or record.get(field)
                if value:
                    unique_values.add(value.strip())

            # Insert each unique value into the corresponding lookup table
            for value in unique_values:
                try:
                    cursor.execute(f"INSERT IGNORE INTO {table} ({column}) VALUES (%s)", (value,))
                    logging.debug(f"Inserted value '{value}' into {table}.{column}")
                except Exception as e:
                    logging.error(f"Error inserting into {table} ({value}): {e}")
                    print(f"Error inserting into {table}: {e}")
                    
            logging.info(f"Loaded {len(unique_values)} unique values into {table}.")
            conn.commit()

        # Close database resources
        cursor.close()
        conn.close()
        logging.info("Database connection closed after leads lookup load.")
    except Exception as e:
        logging.error(f"Failed to load leads lookups: {e}")
        print("Error: Could not load leads lookups.")
