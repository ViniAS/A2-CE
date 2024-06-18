# Conecta em no Banco de Dados PostgreSQL Source e responde a pergunta 1
# Número de produtos comprados por minuto
# return table format: 
# PURCHASE DATE,QUANTITY
# 2024-06-08 12:04:00,6
# 2024-06-08 12:05:00,1

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Answer Q1") \
    .getOrCreate()

# Load configuration from config.json
with open('config.json') as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "../jdbc/postgresql-42.7.3.jar"

url = config['db_source_url']
db_properties = {
    "user": config['db_source_user'],
    "password": config['db_source_password'],
    "driver": "org.postgresql.Driver"
}

# df = spark.read.jdbc(url=url, table="order", properties=db_properties)

df = spark.read.csv('../data/data_mock/order.csv', header=True) # for local testing

def answer_q1(df):
    try:
        df = df.withColumn('purchase_date', F.to_timestamp('purchase_date'))
        df = df.withColumn('minute', F.date_format('purchase_date', 'yyyy-MM-dd HH:mm:00'))
        
        df = df.select(['minute', 'quantity'])
        df = df.groupBy('minute').agg(F.sum('quantity').alias('quantity'))
        df = df.sort('minute')
        return df
    except Exception as e:
        print(f"Error: {e}")

df = answer_q1(df)