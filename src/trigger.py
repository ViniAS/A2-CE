from extractor_log import extract_log
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import time
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"
spark = SparkSession.builder \
        .appName("Extract Log") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()
while True:
    extract_log(spark)
    time.sleep(10)