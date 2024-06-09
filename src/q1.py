import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import os

# Load configuration from config.json
with open('config.json') as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "../jdbc/postgresql-42.7.3.jar"

# Create a Spark session
spark = SparkSession.builder \
    .appName("PySpark PostgreSQL Example") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

# Database connection properties
url = config['db_url'] #TODO Mude o arquivo config.json para o seu banco de dados
properties = {
    "user": config['db_user'],
    "password": config['db_password'],
    "driver": "org.postgresql.Driver"
}

# Read data from PostgreSQL table
try:
    df = spark.read.jdbc(url=url, table="your_table_name", properties=properties)
    
    # Process data (example: filter and select specific columns)
    processed_df = df.filter(df['age'] > 30).select('id', 'name', 'age')
    
    # Show the processed data
    processed_df.show()
    
    # Write processed data back to PostgreSQL table
    processed_df.write.jdbc(url=url, table="processed_table_name", mode="overwrite", properties=properties)
except Exception as e:
    print(f"Error: {e}")
finally:
    # Stop the Spark session
    spark.stop()