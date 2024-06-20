# Registros live (BD)
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
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"

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
MOCK.curr_user_id = 1_000
MOCK.curr_product_id = 1_000

count_time = 0
while True:
    # Every 5 seconds, generate 10 new order data and write it to the database
    time.sleep(1)
    count_time += 1
    # Random int between 1 and 10
    qtd = random.randint(50, 150)
    order_data = [MOCK.order_data(get_new_date=False) for _ in range(qtd)]
    try:
        df = spark.createDataFrame(order_data)
        df.write.jdbc(url=url, table="order_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error: {e}")
        spark.stop()
        break
    if count_time % 5 == 0:
        qtd = random.randint(1, 15)
        user_data = [MOCK.consumer_data() for _ in range(qtd)]
        try:
            df = spark.createDataFrame(user_data)
            df.write.jdbc(url=url, table="consumer_data", mode="append", properties=properties)
        except Exception as e:
            print(f"Error: {e}")
            spark.stop()
            break
    if count_time % 10 == 0:
        count_time = 0
        qtd = random.randint(1, 5)
        product_data = [MOCK.product_data() for _ in range(qtd)]
        try:
            df = spark.createDataFrame(product_data)
            df.write.jdbc(url=url, table="product_data", mode="append", properties=properties)
        except Exception as e:
            print(f"Error: {e}")
            spark.stop()
            break
        stock_data = [MOCK.stock_data() for _ in range(qtd)]
        try:
            df = spark.createDataFrame(stock_data)
            df.write.jdbc(url=url, table="stock_data", mode="append", properties=properties)
        except Exception as e:
            print(f"Error: {e}")
            spark.stop()
            break