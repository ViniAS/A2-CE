# Registros históricos (BD)

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import os
import time
import mock_utils as _mock
import random

# Load configuration from config.json
with open("src/config.json") as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "/usr/share/java/postgresql-42.2.23.jar"

# Create a Spark session
spark = SparkSession.builder \
     .appName("PySpark PostgreSQL Live Data") \
     .config("spark.jars", jdbc_driver_path) \
     .getOrCreate()

# Database connection properties
url = config['db_source_url']
properties = {
    "user": config['db_source_user'],
    "password": config['db_source_password'],
    "driver": "org.postgresql.Driver"
}

MOCK = _mock.MOCK()

# consumer_data
for _ in range(100):
    consumer_data = [MOCK.consumer_data() for _ in range(100)]
    try:
        df = spark.createDataFrame(consumer_data)
        df.write.jdbc(url=url, table="consumer_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error: {e}")
        break

# product_data
for _ in range(100):
    product_data = [MOCK.product_data() for _ in range(100)]
    try:
        df = spark.createDataFrame(product_data)
        df.write.jdbc(url=url, table="product_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error: {e}")
        break

# stock_data
for _ in range(100):
    stock_data = [MOCK.stock_data() for _ in range(100)]
    try:
        df = spark.createDataFrame(stock_data)
        df.write.jdbc(url=url, table="stock_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error: {e}")
        break

for idx in range(10):
    alphabet = "ABCDEFGHIJ"
    shop_data = MOCK.shop_data(idx)
    try:
        df = spark.createDataFrame([shop_data])
        df.write.jdbc(url=url, table="shop_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error SHOP: {e}")
        break

# order_data
for _ in range(1_000):
    order_data = [MOCK.order_data(get_new_date=True) for _ in range(100)]
    try:
        df = spark.createDataFrame(order_data)
        # df.show()
        df.write.jdbc(url=url, table="order_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error: {e}")
        break

spark.stop()