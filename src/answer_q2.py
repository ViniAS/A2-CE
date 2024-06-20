# Conecta no banco de dados PostgreSQL Source e responde a pergunta 2
# Valor faturado por minuto.

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


def answer_q2(spark, store_id=None):
    df = spark.read.jdbc(url=url, table="order_data", properties=db_properties)
    df2 = spark.read.jdbc(url=url, table="product_data", properties=db_properties)
    try:
        if store_id:
            df = df.filter(F.col('shop_id') == store_id)
            df2 = df2.filter(F.col('shop_id') == store_id)
        df = df.withColumnRenamed('product_id', 'p_id')
        df = df.withColumnRenamed('price', 'p_price')
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
        number = df.withColumn('revenue_per_minute', F.col('revenue_per_minute')/(max_minute - min_minute).total_seconds()/60)
        # sum all quantities
        number = number.groupBy().agg(F.sum('revenue_per_minute').alias('revenue_per_minute'))
        return df, number
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Answer Q2") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()
    df, number = answer_q2(spark)
    df.show()
    print(number)
    number.show()