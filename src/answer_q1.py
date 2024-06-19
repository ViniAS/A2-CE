# Conecta em no Banco de Dados PostgreSQL Source e responde a pergunta 1
# Número de produtos comprados por minuto
# return table format: 
# PURCHASE DATE,QUANTITY
# 2024-06-08 12:04:00,6
# 2024-06-08 12:05:00,1

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json

# Load configuration from config.json
with open('src/config.json') as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"

# Cria uma sessão Spark
# spark = SparkSession.builder \
#     .appName("Answer Q1") \
#     .config("spark.jars", jdbc_driver_path) \
#     .getOrCreate()

url = config['db_source_url']
db_properties = {
    "user": config['db_source_user'],
    "password": config['db_source_password'],
    "driver": "org.postgresql.Driver"
}

def answer_q1(spark, store_id=None, table= True):
    df = spark.read.jdbc(url=url, table="order_data", properties=db_properties)
    try:
        df = df.withColumn('purchase_date', F.to_timestamp('purchase_date'))
        min_minute = df.agg({'purchase_date': 'min'}).collect()[0][0]
        max_minute = df.agg({'purchase_date': 'max'}).collect()[0][0]
        df = df.withColumn('minute', F.date_format('purchase_date', 'yyyy-MM-dd HH:mm:00'))
        
        df = df.select(['minute', 'quantity'])
        df = df.groupBy('minute').agg(F.sum('quantity').alias('quantity'))
        df = df.sort('minute')
        if table:
            return df
        else:
            df = df.withColumn('quantity', F.col('quantity')/(max_minute - min_minute).total_seconds()/60)
            # sum all quantities
            df = df.groupBy().agg(F.sum('quantity').alias('quantity'))
        return df
    except Exception as e:
        print(f"Error: {e}")