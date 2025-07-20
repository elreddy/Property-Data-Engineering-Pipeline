import pandas as pd
from db import get_connection
from extraction import extract_json
import numpy as np
import logging

def load_hoa_data(file_path: str) -> None:
    """
    Loads HOA data from a JSON file, processes it, and inserts relevant records into the database.
    """
    try:
        # Establish database connection
        conn = get_connection()
        if conn is None:
            logging.error("Database connection is None.")
            print("Error: Could not connect to the database.")
            return
        cursor = conn.cursor()
        logging.info("Database connection established.")

        # Step 1: Load raw JSON into DataFrame
        try:
            records = extract_json(file_path)
            df = pd.DataFrame(records)
            logging.info(f"Loaded {len(df)} records from {file_path}")
        except Exception as e:
            logging.error(f"Error loading JSON data: {e}")
            print("Error: Could not load JSON data.")
            return

        # Step 2: Fetch property table data
        try:
            cursor.execute("SELECT property_id, property_title FROM property")
            property_rows = cursor.fetchall()
            property_df = pd.DataFrame(property_rows, columns=['property_id', 'property_title'])
            logging.info(f"Fetched {len(property_df)} property records from database.")
        except Exception as e:
            logging.error(f"Error fetching property data: {e}")
            print("Error: Could not fetch property data.")
            cursor.close()
            conn.close()
            return

        # Step 3: Fetch hoa_lookup table data
        try:
            cursor.execute("SELECT hoa_lookup_id, hoa_value, hoa_flag FROM hoa_lookup")
            hoa_rows = cursor.fetchall()
            hoa_lookup_df = pd.DataFrame(hoa_rows, columns=['hoa_lookup_id', 'hoa_value', 'hoa_flag']) 
            logging.info(f"Fetched {len(hoa_lookup_df)} HOA lookup records from database.")
        except Exception as e:
            logging.error(f"Error fetching HOA data: {e}")
            print("Error: Could not fetch HOA data.")
            cursor.close()
            conn.close()
            return

        # Step 4: Prepare HOA DataFrame from JSON records
        columns = [
            "Property_Title",
            "HOA",
            "HOA_Flag"
        ]

        rows = []
        for record in records:
            property_title = record.get("Property_Title")
            hoa_details = record.get("HOA", [])
            for detail in hoa_details:
                HOA = detail.get("HOA")
                HOA_Flag = detail.get("HOA_Flag")
                rows.append([property_title, HOA, HOA_Flag])
        hoa_df = pd.DataFrame(rows, columns=columns)
        logging.info(f"Prepared HOA DataFrame with {len(hoa_df)} rows.")

        # Merge property data with HOA and HOA lookup data
        property_df = property_df.merge(
            hoa_df, left_on='property_title', right_on='Property_Title', how='left', suffixes=('', '_hoa')
        )
        logging.info("Merged property data with HOA DataFrame.")

        property_df = property_df.merge(
            hoa_lookup_df, left_on=['HOA', 'HOA_Flag'], right_on=['hoa_value', 'hoa_flag'], how='left', suffixes=('', '_hoa_lookup')
        )
        logging.info("Merged property data with HOA lookup DataFrame.")

        # Step 5: Prepare data for INSERT into hoa table
        insert_cols = [
            "property_id",
            "hoa_lookup_id"
        ]

        values = property_df[insert_cols]
        columns_str = ', '.join(insert_cols)
        values = values.replace('', None).replace('Null', None)
            
        insert_count = 0
        for value in values.iterrows():
            # Replace NaN, np.nan, or empty string with None for SQL compatibility
            row = [val if not (pd.isna(val) or val is np.nan or val == ' ') else None for val in value[1]]
            try:
                cursor.execute(
                    f"INSERT IGNORE INTO hoa ({columns_str}) VALUES ({', '.join(['%s'] * len(insert_cols))})", row
                )
                insert_count += 1
                logging.debug(f"Inserted row: {row}")
            except Exception as e:
                logging.error(f"Error inserting row into hoa: {e}")
                print(f"Error inserting into hoa: {e}")

        logging.info(f"Inserted {insert_count} rows into hoa table.")
        conn.commit()
        logging.info("Database commit successful.")
    except Exception as e:
        logging.error(f"Failed to load hoa data: {e}")
        print("Error: Could not load hoa data.")
    finally:
        # Ensure resources are closed properly
        try:
            cursor.close()
            logging.info("Database cursor closed.")
        except Exception as e:
            logging.warning(f"Error closing cursor: {e}")
        try:
            conn.close()
            logging.info("Database connection closed.")
        except Exception as e:
            logging.warning(f"Error closing connection: {e}")