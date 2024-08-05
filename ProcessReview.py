import sqlite3
import pyodbc
import pandas as pd
import json
import logging
import sys
from datetime import datetime

# Setup logging
logging.basicConfig(filename='ProcessReview.log',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_file_path):
    with open(config_file_path, 'r') as file:
        config = json.load(file)
    return config

def connect_sqlite(database_path):
    try:
        conn = sqlite3.connect(database_path)
        logging.info("Connected to SQLite database")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to SQLite database: {e}")
        sys.exit(1)

def connect_mssql(server, database, username, password):
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )
        conn = pyodbc.connect(conn_str)
        logging.info("Connected to MS SQL database")
        return conn
    except pyodbc.Error as e:
        logging.error(f"Error connecting to MS SQL database: {e}")
        sys.exit(1)

def normalize_data(df):
    # Convert 'Time' to datetime format
    df['Time'] = pd.to_datetime(df['Time'], unit='s')
    # Drop duplicates
    df.drop_duplicates(inplace=True)
    # Handle missing values (example: fill missing with empty string)
    df.fillna('', inplace=True)
    return df

def main():
    # Load configuration
    config_file_path = './review.json'
    config = load_config(config_file_path)

    # Connect to SQLite
    sqlite_conn = connect_sqlite(config['sqlite']['database'])

    # Connect to MS SQL
    mssql_conn = connect_mssql(
        config['mssql']['server'],
        config['mssql']['database'],
        config['mssql']['username'],
        config['mssql']['password']
    )

    try:
        # Read data from SQLite
        query = "SELECT * FROM Reviews limit 100"
        df = pd.read_sql_query(query, sqlite_conn)
        logging.info("Data read from SQLite database")

        # Normalize and clean data
        df = normalize_data(df)
        logging.info("Data normalization and cleaning completed")

        # Create table if it does not exist
        cursor = mssql_conn.cursor()
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Reviews' AND xtype='U')
        CREATE TABLE Reviews (
            Id INT PRIMARY KEY,
            ProductId NVARCHAR(255),
            UserId NVARCHAR(255),
            ProfileName NVARCHAR(255),
            HelpfulnessNumerator INT,
            HelpfulnessDenominator INT,
            Score INT,
            Time DATETIME,
            Summary NVARCHAR(MAX),
            Text NVARCHAR(MAX)
        )
        """)
        mssql_conn.commit()
        logging.info("Table checked/created in MS SQL database")

        # Prepare data for bulk insert
        data_to_insert = df.values.tolist()
        
        # Bulk insert data into MS SQL
        cursor.executemany("""
        INSERT INTO Reviews (Id, ProductId, UserId, ProfileName, HelpfulnessNumerator, 
        HelpfulnessDenominator, Score, Time, Summary, Text) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, data_to_insert)
        
        mssql_conn.commit()
        logging.info("Data successfully written to MS SQL database")
    except Exception as e:
        logging.error(f"Error processing data: {e}")
    finally:
        sqlite_conn.close()
        mssql_conn.close()
        logging.info("Database connections closed")

if __name__ == "__main__":
    start_time = datetime.now()
    main()
    end_time = datetime.now()
    logging.info(f"Total runtime: {end_time - start_time}")

# Optional: Schedule this script using cron jobs or Apache Airflow
