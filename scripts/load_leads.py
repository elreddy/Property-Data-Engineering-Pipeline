import pandas as pd
import numpy as np
import logging
from db import get_connection
from extraction import extract_json

def get_lookup_df(cursor, table, id_col, value_col):
    try:
        cursor.execute(f"SELECT {id_col}, {value_col} FROM {table}")
        rows = cursor.fetchall()
        logging.info(f"Loaded {len(rows)} rows from {table}")
        return pd.DataFrame(rows, columns=[id_col, value_col])
    except Exception as e:
        logging.error(f"Error loading lookup table {table}: {e}")
        return pd.DataFrame(columns=[id_col, value_col])

def load_lead_data(file_path):
    try:
        conn = get_connection()
        if conn is None:
            logging.error("Database connection is None.")
            print("Error: Could not connect to the database.")
            return
        cursor = conn.cursor()

        # Step 1: Load raw JSON into DataFrame
        try:
            records = extract_json(file_path)
            df = pd.DataFrame(records)
            logging.info(f"Loaded {len(df)} records from {file_path}")
        except Exception as e:
            logging.error(f"Error loading JSON data: {e}")
            print("Error: Could not load JSON data.")
            return

        # Step 2: Load lookup tables
        source_df = get_lookup_df(cursor, 'source_lookup', 'source_id', 'source_name')
        selling_reason_df = get_lookup_df(cursor, 'selling_reason_lookup', 'selling_reason_id', 'selling_reason')
        final_reviewer_df = get_lookup_df(cursor, 'final_reviewer_lookup', 'reviewer_id', 'reviewer_name')

        # Step 3: Map lookups
        try:
            df['Source'] = df['Source'].str.strip()
            source_df['source_name'] = source_df['source_name']
            df = df.merge(source_df, left_on='Source', right_on='source_name', how='left')

            df['Selling_Reason'] = df['Selling_Reason'].str.strip()
            selling_reason_df['selling_reason'] = selling_reason_df['selling_reason']
            df = df.merge(selling_reason_df, left_on='Selling_Reason', right_on='selling_reason', how='left')

            df['Final_Reviewer'] = df['Final_Reviewer'].str.strip()
            final_reviewer_df['reviewer_name'] = final_reviewer_df['reviewer_name']
            df = df.merge(final_reviewer_df, left_on='Final_Reviewer', right_on='reviewer_name', how='left')
            logging.info("Successfully mapped lookup values to DataFrame.")
            
        except Exception as e:
            logging.error(f"Error mapping lookups: {e}")
            print("Error: Could not map lookup values.")
            cursor.close()
            conn.close()
            return

        # Step 4: Prepare INSERT
        insert_cols = [
            'Property_Title', 'Reviewed_Status', 'Most_Recent_Status', 'source_id', 'Occupancy', 'Net_Yield', 'IRR', 
            'selling_reason_id', 'Seller_Retained_Broker','reviewer_id'
        ]
        
        values = df[insert_cols]
        columns = ', '.join(insert_cols)
        values = values.replace('', None).replace('Null', None)

        insert_count = 0
        for value in values.iterrows():
            row = [val if not (pd.isna(val) or val is np.nan or val==' ') else None for val in value[1]]
            try:
                cursor.execute(
                    f"INSERT IGNORE INTO leads ({columns}) VALUES ({', '.join(['%s'] * len(insert_cols))})", row
                )
                insert_count += 1
            except Exception as e:
                logging.error(f"Error inserting row into leads: {e}")
                print(f"Error inserting into leads: {e}")
        logging.info(f"Inserted {insert_count} rows into leads table.")
        conn.commit()
        
        cursor.close()
        conn.close()
        logging.info("Database connection closed successfully.")
    except Exception as e:
        logging.error(f"Failed to load lead data: {e}")
        print("Error: Could not load lead data. Check logs for details.")


    