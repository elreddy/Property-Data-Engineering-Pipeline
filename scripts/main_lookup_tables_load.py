import logging
from hoa_load_lookups import load_hoa_lookup
from leads_load_lookups import load_leads_lookups
from property_load_lookups import load_property_lookups

# Configure logging
logging.basicConfig(
    filename='main_lookup_tables_load.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

if __name__ == "__main__":
    try:
        # Load HOA lookup values from JSON into the database
        load_hoa_lookup('fake_property_data.json')
        print("HOA lookups loaded successfully.")
        logging.info("HOA lookups loaded successfully.")

        # Load leads lookup values (source, selling reason, reviewer) from JSON into the database
        load_leads_lookups('fake_property_data.json')
        print("Leads lookups loaded successfully.")
        logging.info("Leads lookups loaded successfully.")

        # Load property lookup values (market, flood, property type, etc.) from JSON into the database
        load_property_lookups('fake_property_data.json')
        print("Property lookups loaded successfully.")
        logging.info("Property lookups loaded successfully.")
    except Exception as e:
        logging.error(f"Error in main lookup tables load: {e}")
        print("Error: Could not load lookup tables. Check logs for details.")