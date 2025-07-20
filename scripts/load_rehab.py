import pandas as pd
from db import get_connection
from extraction import extract_json
import logging
import numpy as np

def load_rehab_data(file_path):
    """
    Loads rehab data from a JSON file, processes it, merges with property data, and inserts records into the rehab table.
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
            logging.info(f"Loaded {len(records)} records from {file_path}")
        except Exception as e:
            logging.error(f"Error loading JSON data: {e}")
            print("Error: Could not load JSON data.")
            return
        
        # Step 2: Fetch property table data for mapping
        try:
            cursor.execute("SELECT property_id, property_title FROM property")
            property_rows = cursor.fetchall()
            property_df = pd.DataFrame(property_rows, columns=['property_id', 'property_title'])
            logging.info(f"Loaded {len(property_df)} rows from property table")
        except Exception as e:
            logging.error(f"Error fetching property data: {e}")
            print("Error: Could not fetch property data.")
            cursor.close()
            conn.close()
            return
    
        # Step 3: Prepare rehab DataFrame from JSON records
        columns = [
            "Property_Title",
            "Underwriting_Rehab",
            "Rehab_Calculation",
            "Paint",
            "Flooring_Flag",
            "Foundation_Flag",
            "Roof_Flag",
            "HVAC_Flag",
            "Kitchen_Flag",
            "Bathroom_Flag",
            "Appliances_Flag",
            "Windows_Flag",
            "Landscaping_Flag",
            "Trashout_Flag"
        ]
        rows = []
        for record in records:
            property_title = record.get("Property_Title")
            rehab_details = record.get("Rehab", [])
            for detail in rehab_details:
                Underwriting_Rehab = detail.get("Underwriting_Rehab")
                Rehab_Calculation = detail.get("Rehab_Calculation") 
                Paint = detail.get("Paint")
                Flooring_Flag = detail.get("Flooring_Flag") 
                Foundation_Flag = detail.get("Foundation_Flag")
                Roof_Flag = detail.get("Roof_Flag")
                HVAC_Flag = detail.get("HVAC_Flag")
                Kitchen_Flag = detail.get("Kitchen_Flag")
                Bathroom_Flag = detail.get("Bathroom_Flag")
                Appliances_Flag = detail.get("Appliances_Flag")
                Windows_Flag = detail.get("Windows_Flag")
                Landscaping_Flag = detail.get("Landscaping_Flag")
                Trashout_Flag = detail.get("Trashout_Flag")
                rows.append([
                    property_title,
                    Underwriting_Rehab,
                    Rehab_Calculation,
                    Paint,
                    Flooring_Flag,
                    Foundation_Flag,
                    Roof_Flag,
                    HVAC_Flag,
                    Kitchen_Flag,
                    Bathroom_Flag,
                    Appliances_Flag,
                    Windows_Flag,
                    Landscaping_Flag,
                    Trashout_Flag
                ])
        rehab_df = pd.DataFrame(rows, columns=columns)
        logging.info(f"Rehab data extracted with {len(rehab_df)} records.")

        # Step 4: Merge property data with rehab data
        property_df = property_df.merge(rehab_df, left_on='property_title', right_on='Property_Title', how='left', suffixes=('', '_rehab'))
        logging.info("Successfully merged property data with rehab DataFrame.")

        # Step 5: Prepare data for INSERT into rehab table
        insert_cols = [
            "property_id",
            "Underwriting_Rehab",
            "Rehab_Calculation",
            "Paint",
            "Flooring_Flag",
            "Foundation_Flag",
            "Roof_Flag",
            "HVAC_Flag",
            "Kitchen_Flag",
            "Bathroom_Flag",
            "Appliances_Flag",
            "Windows_Flag",
            "Landscaping_Flag",
            "Trashout_Flag"
        ]

        values = property_df[insert_cols]
        columns = ', '.join(insert_cols)
        # Replace empty strings and 'Null' with None for SQL compatibility
        values = values.replace('', None).replace('Null', None)
            
        insert_count = 0
        # Iterate over each row to insert into the rehab table
        for value in values.iterrows():
            # Ensure all NaN, np.nan, or blank values are set to None for SQL
            row = [val if not (pd.isna(val) or val is np.nan or val==' ') else None for val in value[1]]
            try:
                cursor.execute(
                    f"INSERT IGNORE INTO rehab ({columns}) VALUES ({', '.join(['%s'] * len(insert_cols))})", row
                )
                insert_count += 1
                logging.debug(f"Inserted row into rehab: {row}")  # Log each successful insert at debug level
            except Exception as e:
                logging.error(f"Error inserting row into rehab: {e}")
                print(f"Error inserting into rehab: {e}")

        logging.info(f"Inserted {insert_count} rows into rehab table.")  # Log total successful inserts
        conn.commit()
        logging.info("Database commit successful for rehab inserts.")  # Log DB commit
    except Exception as e:
        logging.error(f"Failed to load rehab data: {e}")
        print("Error: Could not load rehab data.")
    finally:
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

