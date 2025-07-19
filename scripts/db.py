# Import necessary libraries
import mysql.connector
import logging

# Function to get a database connection
def get_connection():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            port=3306,
            user="db_user",
            password="6equj5_db_user",
            database="home_db"
        )
        logging.info("Database connection established successfully.")
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Database connection failed: {err}")
        print("Error: Could not connect to the database. Check logs for details.")
