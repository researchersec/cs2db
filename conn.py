import os
import mysql.connector
from mysql.connector import Error

def get_db_connection():
    """Creates a database connection using environment variables."""
    # Fetching connection info from environment variables
    db_url = os.getenv('DB_URL', 'localhost')
    db_port = os.getenv('DB_PORT', '3306')  # Default MySQL port
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    try:
        # Establishing the database connection
        conn = mysql.connector.connect(
            host=db_url,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        if conn.is_connected():
            print("Database connection established.")
            return conn
    except Error as e:
        print(f"Unable to connect to the database. Error: {e}")
        return None

def main():
    # Get a database connection
    conn = get_db_connection()
    if conn and conn.is_connected():
        # Your database operations go here
        # For example, creating a cursor and executing a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION();")
        db_version = cursor.fetchone()
        print(f"Database Version: {db_version[0]}")
        
        # Don't forget to close the cursor and connection when done
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
