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

def benchmark_price_changes(conn):
    """Benchmarks how much the minBuyout prices changed from timestamp to timestamp."""
    query = """
    SELECT
        curr.itemId,
        curr.minBuyout AS current_price,
        prev.minBuyout AS previous_price,
        curr.timestamp AS current_ts,
        prev.timestamp AS previous_ts
    FROM
        pricing_data curr
    INNER JOIN
        pricing_data prev ON curr.itemId = prev.itemId AND prev.timestamp < curr.timestamp
    INNER JOIN (
        SELECT itemId, MAX(timestamp) AS max_ts
        FROM pricing_data
        GROUP BY itemId
    ) grouped_data ON curr.itemId = grouped_data.itemId AND curr.timestamp = grouped_data.max_ts
    WHERE
        prev.timestamp = (
            SELECT MAX(timestamp)
            FROM pricing_data
            WHERE itemId = curr.itemId AND timestamp < curr.timestamp
        )
    ORDER BY
        curr.itemId, curr.timestamp;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)

    results = cursor.fetchall()
    for row in results:
        price_change = row['current_price'] - row['previous_price']
        print(f"ItemId {row['itemId']} changed by {price_change} from {row['previous_ts']} to {row['current_ts']}.")

    cursor.close()
    
def main():
    conn = get_db_connection()
    if conn:
        create_table(conn)
        fetch_and_insert_data(conn)
        benchmark_price_changes(conn)
        conn.close()
        print("Process completed successfully.")

if __name__ == "__main__":
    main()
