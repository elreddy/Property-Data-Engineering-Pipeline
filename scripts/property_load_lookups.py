import logging
from db import get_connection
from extraction import extract_json

# Define the mapping of field → (table, column)
LOOKUPS = {
    "Market": ("market_lookup", "market_name"),
    "Flood": ("flood_lookup", "flood_zone"),
    "Property_Type": ("property_type_lookup", "type_name"),
    "Parking": ("parking_type_lookup", "parking_desc"),
    "Layout": ("layout_type_lookup", "layout_desc"),
    "Subdivision": ("subdivision_lookup", "subdivision_name"),
    "State": ("state_lookup", "state_code"),
}

def load_property_lookups(file_path):
    try:
        data = extract_json(file_path)
        conn = get_connection()
        if conn is None:
            logging.error("Database connection is None.")
            print("Error: Could not connect to the database.")
            return
        cursor = conn.cursor()

        # 1. Simple lookups (no dependencies)
        for field, (table, column) in LOOKUPS.items():
            unique_values = set()
            for record in data:
                value = record.get(field.capitalize()) or record.get(field)
                if value and value.strip():
                    unique_values.add(value.strip())

            for value in unique_values:
                try:
                    cursor.execute(f"INSERT IGNORE INTO {table} ({column}) VALUES (%s)", (value,))
                except Exception as e:
                    logging.error(f"Error inserting into {table} ({value}): {e}")
                    print(f"Error inserting into {table}: {e}")
            logging.info(f"Inserted {len(unique_values)} unique values into {table}.")
            conn.commit()

        # 2. State lookup must be preloaded before city
        state_map = {}
        try:
            cursor.execute("SELECT state_id, state_code FROM state_lookup")
            for sid, code in cursor.fetchall():
                state_map[code] = sid
        except Exception as e:
            logging.error(f"Error fetching state_lookup: {e}")
            print(f"Error fetching state_lookup: {e}")

        # 3. City lookup depends on state_id
        city_set = set()
        for record in data:
            state = record.get("state") or record.get("State")
            city = record.get("city") or record.get("City")
            if city and state:
                state = state.strip()
                city = city.strip()
                if state in state_map:
                    city_set.add((city, state_map[state]))

        for city_name, state_id in city_set:
            try:
                cursor.execute(
                    "INSERT IGNORE INTO city_lookup (city_name, state_id) VALUES (%s, %s)",
                    (city_name, state_id)
                )
            except Exception as e:
                logging.error(f"Error inserting city ({city_name}, {state_id}): {e}")
                print(f"Error inserting city: {e}")
        logging.info(f"Inserted {len(city_set)} unique cities.")
        conn.commit()

        # 4. Address table (depends on city)
        city_map = {}
        try:
            cursor.execute("SELECT city_id, city_name, state_id FROM city_lookup")
            for cid, cname, sid in cursor.fetchall():
                city_map[(cname, sid)] = cid
        except Exception as e:
            logging.error(f"Error fetching city_lookup: {e}")
            print(f"Error fetching city_lookup: {e}")

        address_set = set()
        for record in data:
            street = record.get("street_address") or record.get("Street_Address")
            city = record.get("city") or record.get("City")
            state = record.get("state") or record.get("State")
            zip_code = record.get("zip") or record.get("Zip")
            if all([street, city, state, zip_code]):
                state = state.strip()
                city = city.strip()
                street = street.strip()
                zip_code = zip_code.strip()
                state_id = state_map.get(state)
                city_id = city_map.get((city, state_id))
                if city_id:
                    address_set.add((street, city_id, zip_code))

        for street_address, city_id, zip_code in address_set:
            try:
                cursor.execute(
                    "INSERT IGNORE INTO address (street_address, city_id, zip) VALUES (%s, %s, %s)",
                    (street_address, city_id, zip_code)
                )
            except Exception as e:
                logging.error(f"Error inserting address ({street_address}, {city_id}, {zip_code}): {e}")
                print(f"Error inserting address: {e}")
        logging.info(f"Inserted {len(address_set)} unique addresses.")
        conn.commit()

        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Failed to load property lookups: {e}")
        print("Error: Could not load property lookups. Check logs for details.")
