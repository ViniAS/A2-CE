# Conecta no banco de dados PostgreSQL Source e responde a pergunta 2
#  Valor faturado por minuto.
# return table format:
# REVENUE DATE,REVENUE
# 2024-06-08 10:28:00,802.0589763285572
# 2024-06-08 10:29:00,804.3087244286917
# 2024-06-08 10:30:00,989.1108616859157

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Answer Q2") \
    .getOrCreate()

# Load configuration from config.json
# with open('config.json') as f:
#     config = json.load(f)

# # Path to the PostgreSQL JDBC driver
# jdbc_driver_path = "../jdbc/postgresql-42.7.3.jar"

# url = config['db_source_url']
# db_properties = {
#     "user": config['db_source_user'],
#     "password": config['db_source_password'],
#     "driver": "org.postgresql.Driver"
# }

# df = spark.read.jdbc(url=url, table="order", properties=db_properties)
# df2 = spark.read.jdbc(url=url, table="product", properties=db_properties)


def answer_q2(store_id=None, table=True):
    df = spark.read.csv('data/data_mock/order.csv', header=True)
    df2 = spark.read.csv('data/data_mock/product.csv', header=True)
    try:
        df = df.withColumnRenamed('product_id', 'p_id')
        df = df.join(df2, df['p_id'] == df2['product_id'])
        df = df.withColumn('quantity', df['quantity'].cast('int'))
        df = df.withColumn('price', df['price'].cast('float'))
        df = df.withColumn('purchase_date', F.to_timestamp('purchase_date'))
        min_minute = df.agg({'purchase_date': 'min'}).collect()[0][0]
        max_minute = df.agg({'purchase_date': 'max'}).collect()[0][0]
        df = df.withColumn('minute',F.date_format('purchase_date', 'yyyy-MM-dd HH:mm:00'))
        df = df.select(['minute', 'quantity', 'price'])
        df = df.withColumn('revenue', df['quantity'] * df['price'])
        df = df.groupBy('minute').agg(F.sum('revenue').alias('revenue_per_minute'))
        df = df.sort('minute')
        if table:
            return df
        else:
            df = df.withColumn('revenue_per_minute', F.col('revenue_per_minute')/(max_minute - min_minute).total_seconds()/60)
            # sum all quantities
            df = df.groupBy().agg(F.sum('revenue_per_minute').alias('revenue_per_minute'))
        return df
    except Exception as e:
        print(f"Error: {e}")