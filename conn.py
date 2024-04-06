import os
import requests
import mysql.connector
from mysql.connector import Error

def get_db_connection():
    """Creates a database connection using environment variables."""
    db_url = os.getenv('DB_URL', 'localhost')
    db_port = os.getenv('DB_PORT', '3306')  # Default MySQL port
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    try:
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

def create_table(conn):
    """Creates the pricing_data table without auctionHouseId and petSpeciesId."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS pricing_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        itemId INT,
        minBuyout INT,
        quantity INT,
        marketValue INT,
        historical INT,
        numAuctions INT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    print("Table checked/created successfully.")

def fetch_and_insert_data(conn):
    """Fetches pricing data and inserts it using batch operation."""
    data_url = "https://raw.githubusercontent.com/researchersec/lonewolf/main/prices/latest.json"
    response = requests.get(data_url)
    pricing_data = response.json()['pricing_data']

    insert_sql = """
    INSERT INTO pricing_data 
    (itemId, minBuyout, quantity, marketValue, historical, numAuctions) 
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    # Prepare the data to insert
    data_to_insert = [
        (
            item.get('itemId'),
            item.get('minBuyout'),
            item.get('quantity'),
            item.get('marketValue'),
            item.get('historical'),
            item.get('numAuctions')
        )
        for item in pricing_data
    ]

    cursor = conn.cursor()

    # Use executemany to perform batch insertion
    cursor.executemany(insert_sql, data_to_insert)

    conn.commit()
    cursor.close()
    print(f"{len(data_to_insert)} records inserted successfully.")

def main():
    conn = get_db_connection()
    if conn:
        create_table(conn)
        fetch_and_insert_data(conn)
        conn.close()
        print("Process completed successfully.")

if __name__ == "__main__":
    main()
