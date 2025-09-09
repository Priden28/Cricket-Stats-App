# populate_db.py
import mysql.connector
import pandas as pd
import logging
from datetime import datetime
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database configuration from environment variables
DB_CONFIG = {
    'host': os.getenv("DB_HOST", "mysql"),
    'user': os.getenv("DB_USER", "cricket_user"),
    'password': os.getenv("DB_PASSWORD", "cricket_password"),
    'database': os.getenv("DB_NAME", "cricket_stats")
}

def get_db_connection():
    """Establishes a connection to the MySQL database."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        logging.info("Successfully connected to the database.")
        return conn
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to database: {err}")
        return None

def process_and_insert_data(file_name, table_name, conn):
    """Truncates the table and inserts data from a CSV file."""
    try:
        if not os.path.exists(file_name):
            logging.error(f"File not found: {file_name}")
            return

        df = pd.read_csv(file_name)
        cursor = conn.cursor()

        # Truncate the table to clear old data
        logging.info(f"Truncating table '{table_name}'...")
        cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        conn.commit()
        logging.info(f"Table '{table_name}' truncated successfully.")

        # Prepare for bulk insertion
        inserted_count = 0
        
        # Convert 'Start Date' to a proper datetime and then to date objects
        # This handles both 'YYYY-MM-DD HH:MM:SS' and 'YYYY-MM-DD' formats
        df['Start Date'] = pd.to_datetime(df['Start Date']).dt.date

        # Prepare the SQL INSERT statement
        if table_name == 'team':
            insert_query = """
            INSERT INTO team (`Team`, `ScoreDescending`, `Overs`, `RPO`, `Lead`, `Inns`, 
                            `Result`, `Opposition`, `Ground`, `Start Date`, `Declared`, `Wickets`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            data_to_insert = df[[
                'Team', 'ScoreDescending', 'Overs', 'RPO', 'Lead', 'Inns', 
                'Result', 'Opposition', 'Ground', 'Start Date', 'Declared', 'Wickets'
            ]].fillna(0).values.tolist()
            
        elif table_name == 'batting':
            insert_query = """
            INSERT INTO batting (`Player`, `RunsDescending`, `BF`, `4s`, `6s`, `SR`, `Inns`,
                               `Opposition`, `Ground`, `Start Date`, `Not Out`, `Team`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            data_to_insert = df[[
                'Player', 'RunsDescending', 'BF', '4s', '6s', 'SR', 'Inns',
                'Opposition', 'Ground', 'Start Date', 'Not Out', 'Team'
            ]].fillna(0).values.tolist()

        elif table_name == 'bowling':
            insert_query = """
            INSERT INTO bowling (`Player`, `Overs`, `Mdns`, `Runs`, `WktsDescending`, `Econ`,
                               `Inns`, `Opposition`, `Ground`, `Start Date`, `Team`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            data_to_insert = df[[
                'Player', 'Overs', 'Mdns', 'Runs', 'WktsDescending', 'Econ',
                'Inns', 'Opposition', 'Ground', 'Start Date', 'Team'
            ]].fillna(0).values.tolist()
            
        else:
            logging.error(f"Unknown table name: {table_name}")
            return
            
        logging.info(f"Inserting data into '{table_name}'...")
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        inserted_count = cursor.rowcount
        logging.info(f"Successfully inserted {inserted_count} rows into '{table_name}'.")

    except Exception as e:
        logging.error(f"Error processing {table_name} data: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()

if __name__ == '__main__':
    conn = get_db_connection()
    if conn:
        # Load data from CSVs into tables, assuming files are in the same directory
        process_and_insert_data('team.csv', 'team', conn)
        process_and_insert_data('batting.csv', 'batting', conn)
        process_and_insert_data('bowling.csv', 'bowling', conn)
        
        conn.close()
        logging.info("Database connection closed.")