
# Import necessary libraries
import mysql.connector

# Function to get a database connection
def get_connection():
    connection = mysql.connector.connect(
        host="localhost",
        port=3306,
        user="db_user",
        password="6equj5_db_user",
        database="home_db"
    )
    return connection
