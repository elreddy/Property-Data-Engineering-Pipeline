import pandas as pd
from db import get_connection
from extraction import extract_json
import logging
import numpy as np

def load_valuation_data(file_path):
    """
    Loads valuation data from a JSON file, merges with property data, and inserts records into the valuation table.
    """
    try:
        # Establish database connection
        conn = get_connection()
        if conn is None:
            logging.error("Database connection is None.")
            print("Error: Could not connect to the database.")
            return
        cursor = conn.cursor()

        # Step 1: Load raw JSON into DataFrame
        try:
            records = extract_json(file_path)
            logging.info(f"Loaded {len(records)} records from {file_path}")
        except Exception as e:
            logging.error(f"Error loading JSON data: {e}")
            print("Error: Could not load JSON data.")
            return
        
        # Step 2: Fetch property table data
        try:
            cursor.execute("SELECT property_id, property_title FROM property")
            property_rows = cursor.fetchall()
            property_df = pd.DataFrame(property_rows, columns= ['property_id', 'property_title'])
            logging.info(f"Loaded {len(property_df)} rows from property table")
        except Exception as e:
            logging.error(f"Error fetching property data: {e}")
            print("Error: Could not fetch property data.")
            cursor.close()
            conn.close()
            return
    
        # Step 3: Prepare valuation DataFrame from JSON records
        columns = [
            "Property_Title",
            "Previous_Rent",
            "List_Price",
            "Zestimate",
            "ARV",
            "Expected_Rent",
            "Rent_Zestimate",
            "Low_FMR",
            "High_FMR",
            "Redfin_Value"
        ]
        rows= []
        for record in records:
            property_title = record.get("Property_Title")
            valuation_details = record.get("Valuation", [])
            for detail in valuation_details:
                Previous_Rent = detail.get("Previous_Rent")
                List_Price = detail.get("List_Price")   
                Zestimate = detail.get("Zestimate")
                ARV = detail.get("ARV")
                Expected_Rent = detail.get("Expected_Rent")
                Rent_Zestimate = detail.get("Rent_Zestimate")   
                Low_FMR = detail.get("Low_FMR")
                High_FMR = detail.get("High_FMR")
                Redfin_Value = detail.get("Redfin_Value")
                rows.append([
                    property_title,
                    Previous_Rent,
                    List_Price,
                    Zestimate,
                    ARV,
                    Expected_Rent,
                    Rent_Zestimate, 
                    Low_FMR,
                    High_FMR,   
                    Redfin_Value
                ])
        valuation_df = pd.DataFrame(rows, columns=columns)
        logging.info(f"Valuation data extracted with {len(valuation_df)} records.")

        # Step 4: Merge property data with valuation data
        property_df = property_df.merge(valuation_df, left_on='property_title', right_on='Property_Title', how='left', suffixes=('', '_valuation'))
        logging.info("Successfully merged property data with valuation DataFrame.")

        # Step 5: Prepare and insert data into valuation table
        insert_cols = [
            "property_id",
            "Previous_Rent",
            "List_Price",
            "Zestimate",
            "ARV",
            "Expected_Rent",
            "Rent_Zestimate",
            "Low_FMR",
            "High_FMR",
            "Redfin_Value"
        ]

        values = property_df[insert_cols]
        columns = ', '.join(insert_cols)
        values = values.replace('', None).replace('Null', None)
            
        insert_count = 0
        for value in values.iterrows():
            # Replace NaN, np.nan, or blank with None for SQL compatibility
            row = [val if not (pd.isna(val) or val is np.nan or val==' ') else None for val in value[1]]
            try:
                cursor.execute(
                    f"INSERT IGNORE INTO valuation ({columns}) VALUES ({', '.join(['%s'] * len(insert_cols))})", row
                )
                insert_count += 1
                logging.debug(f"Inserted row into valuation: {row}")  # Log each successful insert at debug level
            except Exception as e:
                logging.error(f"Error inserting row into valuation: {e}")
                print(f"Error inserting into valuation: {e}")

        logging.info(f"Inserted {insert_count} rows into valuation table.")
        conn.commit()
        logging.info("Database commit successful for valuation inserts.")
    except Exception as e:
        logging.error(f"Failed to load valuation data: {e}")
        print("Error: Could not load valuation data.")
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

