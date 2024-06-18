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

# order_data
for _ in range(20_000):
    order_data = [MOCK.order_data(get_new_date=True) for _ in range(1000)]
    try:
        df = spark.createDataFrame(order_data)
        df.write.jdbc(url=url, table="order_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error: {e}")
        break

# consumer_data
for _ in range(5_000):
    consumer_data = [MOCK.consumer_data() for _ in range(1000)]
    try:
        df = spark.createDataFrame(consumer_data)
        df.write.jdbc(url=url, table="consumer_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error: {e}")
        break

# stock_data
for _ in range(5_000):
    stock_data = [MOCK.stock_data() for _ in range(1000)]
    try:
        df = spark.createDataFrame(stock_data)
        df.write.jdbc(url=url, table="stock_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error: {e}")
        break

# product_data
for _ in range(5_000):
    product_data = [MOCK.product_data() for _ in range(1000)]
    try:
        df = spark.createDataFrame(product_data)
        df.write.jdbc(url=url, table="product_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error: {e}")
        break

for _ in range(10):
    alphabet = "ABCDEFGHIJ"
    shop_data = [[i, alphabet[i]*3] for i in range(1, 11)]
    try:
        df = spark.createDataFrame(shop_data)
        df.write.jdbc(url=url, table="shop_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error: {e}")
        break

spark.stop()