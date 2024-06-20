# Conecta em no Banco de Dados PostgreSQL Source e responde a pergunta 1
# Número de produtos comprados por minuto

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json

# Load configuration from config.json
with open('src/config.json') as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"

url = config['db_source_url']
db_properties = {
    "user": config['db_source_user'],
    "password": config['db_source_password'],
    "driver": "org.postgresql.Driver"
}

def answer_q1(spark, store_id=None):
    df = spark.read.jdbc(url=url, table="order_data", properties=db_properties)
    try:
        if store_id:
            df = df.filter(df['shop_id'] == store_id)
        df = df.withColumn('purchase_date', F.to_timestamp('purchase_date'))
        min_minute = df.agg({'purchase_date': 'min'}).collect()[0][0]
        max_minute = df.agg({'purchase_date': 'max'}).collect()[0][0]
        df = df.withColumn('minute', F.date_format('purchase_date', 'yyyy-MM-dd HH:mm:00'))
        
        df = df.select(['minute', 'quantity'])
        df = df.groupBy('minute').agg(F.sum('quantity').alias('quantity'))
        df = df.sort('minute')
        number = df.withColumn('quantity', F.col('quantity')/(max_minute - min_minute).total_seconds()/60)
        # sum all quantities
        number = number.groupBy().agg(F.sum('quantity').alias('quantity'))
        return df, number
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Answer Q1") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()
    df, number = answer_q1(spark)
    df.show()
    print(number)
    number.show()