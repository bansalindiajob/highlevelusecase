from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import snowflake.connector
import json
import logging
import sys
from datetime import datetime

# Initialize logging
logging.basicConfig(filename='processRestaurentData.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

def log_and_print(message):
    logging.info(message)
    print(message)

try:
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("RestaurentDataLoader") \
        .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4") \
        .getOrCreate()
    log_and_print("Spark session initialized")

    # Load the data
    file_path = "./dataset/out/Restaurent.txt"
    df = spark.read.option("delimiter", "|").csv(file_path, header=True)
    log_and_print(f"Data loaded from {file_path}")

    # Data cleaning and normalization
    df = df.na.fill("")  # Handling missing values
    df = df.withColumn("RestaurentName", col("RestaurentName").trim().lower())
    df = df.withColumn("City", col("City").trim().lower())
    df = df.withColumn("Link", col("Link").trim().lower())
    log_and_print("Data cleaning and normalization completed")

    # Create distinct dataset 1
    distinct_df1 = df.select(
        col("RestaurentID"),
        col("RestaurentName"),
        col("Rating"),
        col("Rating_Count"),
        col("Address"),
        col("Loc_No"),
        col("City"),
        col("Link")
    ).distinct()
    log_and_print("Distinct Restaurent created")

    # Create distinct dataset 2
    distinct_df2 = df.select(
        col("RestaurentID"),
        col("MenuCategory"),
        col("MenuCartItem"),
        col("MenuCartItemPrice"),
        col("MenuCartItemType")
    )
    log_and_print("Resturent based Menu dataset created")

    # Function to load Snowflake connection parameters from a JSON file
    def load_snowflake_config(config_file_path):
        with open(config_file_path, 'r') as f:
            config = json.load(f)
        
        sf_options = {
            "sfURL": f"{config['account']}.us-east-1.snowflakecomputing.com",
            "sfAccount": config['account'],
            "sfUser": config['user'],
            "sfDatabase": config['database'],
            "sfSchema": config['schema'],
            "sfWarehouse": config['warehouse'],
            "sfRole": config['role'],
            "sfAuthenticator": "externalbrowser",
            "sfRegion": config.get('region', 'us-east-1')
        }
        return sf_options

    # Load Snowflake configuration
    config_file_path = './config.json'
    sf_options = load_snowflake_config(config_file_path)
    log_and_print("Snowflake configuration loaded")

    # Write distinct dataset 1 to Snowflake
    log_and_print("Writing distinct dataset 1 to Snowflake")
    distinct_df1.write.format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "restaurent") \
        .mode("overwrite") \
        .save()

    # Write distinct dataset 2 to Snowflake
    log_and_print("Writing distinct dataset 2 to Snowflake")
    distinct_df2.write.format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "restaurant_menu") \
        .mode("overwrite") \
        .save()

    # Stop the Spark session
    spark.stop()
    log_and_print("Spark session stopped")

    # Record the end time and calculate the duration
    end_time = time.time()
    log_and_print(f"Total runtime: {end_time - start_time} seconds")

except Exception as e:
    logging.error(f"An error occurred: {e}")
    print(f"An error occurred: {e}")
    sys.exit(1)
