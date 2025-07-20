from load_leads import load_lead_data
from load_property import load_property_data
from load_taxes import load_taxes_data
from load_rehab import load_rehab_data
from load_valuation import load_valuation_data
from load_hoa import load_hoa_data
import logging 

# Configure logging for the script
logging.basicConfig(
    filename='main_tables_load.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

if __name__ == "__main__":
    try:
        # Set the input file name
        file_name = 'fake_property_data.json'
        logging.info(f"Starting main tables load with file: {file_name}")

        # Load lead data into the leads table
        load_lead_data(file_name)
        print("Lead data loaded successfully.")
        logging.info("Lead data loaded successfully.")

        # Load property data into the property table
        load_property_data(file_name)
        print("Property data loaded successfully.")
        logging.info("Property data loaded successfully.")
        
        # Load taxes data into the taxes table
        load_taxes_data(file_name)
        print("Taxes data loaded successfully.")
        logging.info("Taxes data loaded successfully.")

        # Load rehab data into the rehab table
        load_rehab_data(file_name)
        print("Rehab data loaded successfully.")
        logging.info("Rehab data loaded successfully.")
        
        # Load valuation data into the valuation table
        load_valuation_data(file_name)
        print("Valuation data loaded successfully.")
        logging.info("Valuation data loaded successfully.")
        
        # Load HOA data into the hoa table
        load_hoa_data(file_name)
        print("HOA data loaded successfully.")
        logging.info("HOA data loaded successfully.")

        # Log completion of all table loads
        logging.info("Main tables load completed successfully.")

    except Exception as e:
        # Log any exception that occurs during the main tables loading process
        logging.error(f"Error in main tables load: {e}")
        print("Error: Could not load main tables. Check logs for details.")