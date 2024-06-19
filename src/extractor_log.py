# Usa o Spark para acessar um diretório (s3 ou local) e extrair os arquivos de log dele e enviar para o 
# banco de dados PostgreSQL Target

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType


# Path para o diretório de logs
log_dir = "data"
# s3_log_dir = "s3://bucket-name/logs"

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"


# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Extract Log") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

# Define schema based on your database requirements
schema = StructType([
    StructField("user_author_id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("button_product_id", IntegerType(), True),
    StructField("stimulus", StringType(), True),
    StructField("component", StringType(), True),
    StructField("text_content", StringType(), True),
    StructField("date", TimestampType(), True),
])
log_user_behavior = spark.read.csv(log_dir+"/behaviour/", header=False, schema =schema)
column_names = ["user_author_id","action", "button_product_id",
                "stimulus", "component", "text_content", "date"]
log_user_behavior = log_user_behavior.toDF(*column_names)

schema = StructType([
    StructField("message", StringType(), True),
    StructField("text_content", StringType(), True),
    StructField("date", TimestampType(), True),
])
log_debug = spark.read.csv(log_dir+"/debug/", header=False, schema =schema)
column_names = ["message", "text_content", "date"]
log_debug = log_debug.toDF(*column_names)

schema = StructType([
    StructField("component", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("message", StringType(), True),
    StructField("text_content", StringType(), True),
    StructField("date", TimestampType(), True),
])
log_failure = spark.read.csv(log_dir+"/fail/", header=False, schema =schema)
column_names = ["component", "severity", "message", "text_content", "date"]
log_failure = log_failure.toDF(*column_names)

schema = StructType([
    StructField("user_author_id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("action_description", StringType(), True),
    StructField("text_content", StringType(), True),
    StructField("date", TimestampType(), True),
])
log_audit = spark.read.csv(log_dir+"/audit/", header=False, schema =schema)
column_names = ["user_author_id", "action", "action_description", 
                "text_content", "date"]
log_audit = log_audit.toDF(*column_names)

# Load configuration from config.json
with open('src/config.json') as f:
    config = json.load(f)

url = config['db_target_url']
db_properties = {
    "user": config['db_target_user'],
    "password": config['db_target_password'],
    "driver": "org.postgresql.Driver"
}

#writes the data to a PostgreSQL database
log_user_behavior.write.jdbc(url=url, table="user_behavior_log", mode="append", properties=db_properties)
log_debug.write.jdbc(url=url, table="log_debug", mode="append", properties=db_properties)
log_failure.write.jdbc(url=url, table="log_failure", mode="append", properties=db_properties)
log_audit.write.jdbc(url=url, table="log_audit", mode="append", properties=db_properties)

spark.stop()