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

def load_property_data(file_path):
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

        # Step 3: Load lookup tables
        try:
            market_df = get_lookup_df(cursor, 'market_lookup', 'market_id', 'market_name')
            flood_df = get_lookup_df(cursor, 'flood_lookup', 'flood_id', 'flood_zone')
            type_df = get_lookup_df(cursor, 'property_type_lookup', 'type_id', 'type_name')
            parking_df = get_lookup_df(cursor, 'parking_type_lookup', 'parking_id', 'parking_desc')
            layout_df = get_lookup_df(cursor, 'layout_type_lookup', 'layout_id', 'layout_desc')
            subdivision_df = get_lookup_df(cursor, 'subdivision_lookup', 'subdivision_id', 'subdivision_name')
            state_df = get_lookup_df(cursor, 'state_lookup', 'state_id', 'state_code')
            logging.info("Lookup tables loaded successfully.")
        except Exception as e:
            logging.error(f"Error loading lookup tables: {e}")
            print("Error: Could not load lookup tables.")
            cursor.close()
            conn.close()
            return

        try:
            cursor.execute("SELECT lead_id, property_title FROM leads")
            lead_rows = cursor.fetchall()
            leads_df = pd.DataFrame(lead_rows, columns=['lead_id', 'property_title'])
            logging.info(f"Loaded {len(leads_df)} rows from leads table")

            cursor.execute("SELECT city_id, city_name, state_id FROM city_lookup")
            city_rows = cursor.fetchall()
            city_df = pd.DataFrame(city_rows, columns=['city_id', 'city_name', 'state_id'])
            logging.info(f"Loaded {len(city_df)} rows from city_lookup table")

            cursor.execute("SELECT address_id, street_address, city_id, zip FROM address")
            address_rows = cursor.fetchall()
            address_df = pd.DataFrame(address_rows, columns=['address_id', 'street_address', 'city_id', 'zip'])
            logging.info(f"Loaded {len(address_df)} rows from address table")
        except Exception as e:
            logging.error(f"Error loading leads/city/address tables: {e}")
            print("Error: Could not load leads/city/address tables.")
            cursor.close()
            conn.close()
            return

        # Prepare for matching
        try:
            df['Street_Address'] = df['Street_Address'].str.strip()
            df['City'] = df['City'].str.strip()
            df['State'] = df['State'].str.strip()
            df['Zip'] = df['Zip'].astype(str).str.strip()

            city_df['city_name'] = city_df['city_name'].str.strip()
            state_df['state_code'] = state_df['state_code'].str.strip()

            # Merge to get state_id
            df = df.merge(state_df, left_on='State', right_on='state_code', how='left')
            # Merge to get city_id
            df = df.merge(city_df, left_on=['City', 'state_id'], right_on=['city_name', 'state_id'], how='left', suffixes=('', '_city'))
            # Merge to get address_id
            address_df['street_address'] = address_df['street_address'].str.strip()
            address_df['zip'] = address_df['zip'].astype(str).str.strip()
            df = df.merge(address_df, left_on=['Street_Address', 'city_id', 'Zip'], right_on=['street_address', 'city_id', 'zip'], how='left', suffixes=('', '_address'))
            logging.info("Successfully merged address, city, and state data.")
        except Exception as e:
            logging.error(f"Error merging address/city/state data: {e}")
            print("Error: Could not merge address/city/state data.")
            cursor.close()
            conn.close()
            return

        # Step 4: Map lookups
        try:
            df['Property_Title'] = df['Property_Title'].str.strip()
            leads_df['property_title'] = leads_df['property_title'].str.strip()
            df = df.merge(leads_df, left_on='Property_Title', right_on='property_title', how='left', suffixes=('', '_lead'))
            logging.info("Successfully mapped property titles to leads.")

            df['Market'] = df['Market'].str.strip()
            market_df['market_name'] = market_df['market_name']
            df = df.merge(market_df, left_on='Market', right_on='market_name', how='left')
            logging.info("Successfully mapped market names to DataFrame.")

            df['Flood'] = df['Flood'].str.strip()
            flood_df['flood_zone'] = flood_df['flood_zone']
            df = df.merge(flood_df, left_on='Flood', right_on='flood_zone', how='left')
            logging.info("Successfully mapped flood zones to DataFrame.")

            df['Property_Type'] = df['Property_Type'].str.strip()
            type_df['type_name'] = type_df['type_name']
            df = df.merge(type_df, left_on='Property_Type', right_on='type_name', how='left')
            logging.info("Successfully mapped property types to DataFrame.")

            df['Parking'] = df['Parking'].str.strip()
            parking_df['parking_desc'] = parking_df['parking_desc']
            df = df.merge(parking_df, left_on='Parking', right_on='parking_desc', how='left')
            logging.info("Successfully mapped parking types to DataFrame.") 

            df['Layout'] = df['Layout'].str.strip()
            layout_df['layout_desc'] = layout_df['layout_desc']
            df = df.merge(layout_df, left_on='Layout', right_on='layout_desc', how='left')
            logging.info("Successfully mapped layout types to DataFrame.")

            df['Subdivision'] = df['Subdivision'].str.strip()
            subdivision_df['subdivision_name'] = subdivision_df['subdivision_name']
            df = df.merge(subdivision_df, left_on='Subdivision', right_on='subdivision_name', how='left')
            logging.info("Successfully mapped subdivisions to DataFrame.")
        except Exception as e:
            logging.error(f"Error mapping lookups: {e}")
            print("Error: Could not map lookup values.")
            cursor.close()
            conn.close()
            return

        # Step 6: Prepare INSERT
        try:
            insert_cols = [
                'Property_Title',      
                'address_id',          
                'lead_id',             
                'market_id',           
                'flood_id',            
                'type_id',            
                'Highway',            
                'Train',               
                'Tax_Rate',            
                'SQFT_Basement',      
                'HTW',                
                'Pool',                
                'Commercial',        
                'Water',             
                'Sewage',              
                'Year_Built',          
                'SQFT_MU',            
                'SQFT_Total',         
                'parking_id',          
                'Bed',                 
                'Bath',                
                'BasementYesNo',       
                'layout_id',           
                'Rent_Restricted',     
                'Neighborhood_Rating', 
                'Latitude',            
                'Longitude',           
                'subdivision_id',      
                'School_Average'       
            ]
            values = df[insert_cols]
            # Prepare column names for SQL insert
            columns = ', '.join(insert_cols)
            # Replace empty strings and 'Null' with None for SQL compatibility
            values = values.replace('', None).replace('Null', None)
            
            insert_count = 0
            # Iterate over each row to insert into the property table
            for value in values.iterrows():
                # Ensure all NaN, np.nan, or blank values are set to None for SQL
                row = [val if not (pd.isna(val) or val is np.nan or val==' ') else None for val in value[1]]
                try:
                    cursor.execute(
                        f"INSERT IGNORE INTO property ({columns}) VALUES ({', '.join(['%s'] * len(insert_cols))})", row
                    )
                    insert_count += 1
                    logging.debug(f"Inserted row into property: {row}")  # Log each successful insert at debug level
                except Exception as e:
                    logging.error(f"Error inserting row into property: {e}")
                    print(f"Error inserting into property: {e}")

            logging.info(f"Inserted {insert_count} rows into property table.")  # Log total successful inserts
            conn.commit()
            logging.info("Database commit successful for property inserts.")  # Log DB commit
        except Exception as e:
            logging.error(f"Error during property insert: {e}")
            print("Error: Could not insert property data.")
        finally:
            cursor.close()
            conn.close()
            logging.info("Database connection closed successfully.")
    except Exception as e:
        logging.error(f"Failed to load property data: {e}")
        print("Error: Could not load property data.")

