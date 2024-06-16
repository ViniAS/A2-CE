# Registros live (BD)
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import os

# Load configuration from config.json
with open("/src/config.json") as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "/jdbc/postgresql-42.7.3.jar"

# Create a Spark session
spark = SparkSession.builder \
     .appName("PySpark PostgreSQL Example") \
     .config("spark.jars", jdbc_driver_path) \
     .getOrCreate()

# Database connection properties
url = config['db_url']
properties = {
    "user": config['db_user'],
    "password": config['db_password'],
    "driver": "org.postgresql.Driver"
}

try:
    # Generate live data
    df = spark.read.jdbc(url=url, table="live_data", properties=properties)

    # Process data (example: filter and select specific columns)
