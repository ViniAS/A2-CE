# Registros históricos (BD)

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import os
import time
import mock_utils as MOCK
import random

# Load configuration from config.json
with open("src/config.json") as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"

# Create a Spark session
spark = SparkSession.builder \
     .appName("PySpark PostgreSQL Live Data") \
     .config("spark.jars", jdbc_driver_path) \
     .getOrCreate()

# Database connection properties
url = config['db_url']
properties = {
    "user": config['db_user'],
    "password": config['db_password'],
    "driver": "org.postgresql.Driver"
}

for _ in range(300_000):
    order_data = [MOCK.order_data(get_new_date=True) for _ in range(1000)]
    try:
        df = spark.createDataFrame(order_data)
        df.write.jdbc(url=url, table="order_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error: {e}")
        spark.stop()
        break