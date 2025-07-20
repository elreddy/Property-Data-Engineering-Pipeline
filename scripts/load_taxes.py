import pandas as pd
from db import get_connection
from extraction import extract_json
import logging
import numpy as np

def get_lookup_df(cursor, table, id_col, value_col):
    """
    Helper function to fetch lookup tables from the database and return as DataFrame.
    """
    try:
        cursor.execute(f"SELECT {id_col}, {value_col} FROM {table}")
        rows = cursor.fetchall()
        logging.info(f"Loaded {len(rows)} rows from {table}")
        return pd.DataFrame(rows, columns=[id_col, value_col])
    except Exception as e:
        logging.error(f"Error loading lookup table {table}: {e}")
        return pd.DataFrame(columns=[id_col, value_col])
    
def load_taxes_data(file_path):
    """
    Loads taxes data from a JSON file, merges with property data, and inserts records into the taxes table.
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
        
        # Step 2: Fetch property table data for mapping
        try:
            cursor.execute("SELECT property_id, property_title FROM property")
            property_rows = cursor.fetchall()
            property_df = pd.DataFrame(property_rows, columns= ['property_id', 'property_title'])
            logging.info(f"Fetched {len(property_df)} property records from database.")
        except Exception as e:
            logging.error(f"Error fetching property data: {e}")
            print("Error: Could not fetch property data.")
            cursor.close()
            conn.close()
            return
           
        # Step 3: Merge property data with taxes data
        df = df.merge(property_df, left_on='Property_Title', right_on='property_title', how='left', suffixes=('', '_property'))
        logging.info("Successfully merged property data with taxes DataFrame.")

        # Step 4: Prepare and insert data into taxes table
        values = df[['property_id', 'Taxes']]
        values = values.replace('', None).replace('Null', None)
            
        insert_count = 0
        for value in values.iterrows():
            # Replace NaN, np.nan, or blank with None for SQL compatibility
            row = [val if not (pd.isna(val) or val is np.nan or val==' ') else None for val in value[1]]
            try:
                cursor.execute(
                    f"INSERT IGNORE INTO taxes (property_id, tax_value) VALUES ('%s','%s')", row
                )
                insert_count += 1
                logging.debug(f"Inserted row into taxes: {row}")  # Log each successful insert at debug level
            except Exception as e:
                logging.error(f"Error inserting row into taxes: {e}")
                print(f"Error inserting into taxes: {e}")

        logging.info(f"Inserted {insert_count} rows into taxes table.")  # Log total successful inserts
        conn.commit()
        logging.info("Database commit successful for taxes inserts.")  # Log DB commit
    except Exception as e:
        logging.error(f"Error in load_taxes_data: {e}")
        print("Error: Could not load taxes data. Check logs for details.")
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

