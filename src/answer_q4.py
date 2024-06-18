# Conecta nos Bancos de Dados PostgreSQL Source e Target e responde a pergunta 4
#  Ranking de produtos mais visualizados na última hora
# return table format:
# PRODUCT ID,VIEW COUNT,PRODUCT NAME,STORE ID,STORE NAME
# 73,3,Product 73,2,Store 2
# 49,3,Product 49,9,Store 9
# 65,2,Product 65,10,Store 10

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import time

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Answer Q4") \
    .getOrCreate()

# Load configuration from config.json
with open('config.json') as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "../jdbc/postgresql-42.7.3.jar"

url_target = config['db_target_url']
db_properties_target = {
    "user": config['db_target_user'],
    "password": config['db_target_password'],
    "driver": "org.postgresql.Driver"
}

url_source = config['db_source_url']
db_properties_source = {
    "user": config['db_source_user'],
    "password": config['db_source_password'],
    "driver": "org.postgresql.Driver"
}

# df_behavior = spark.read.jdbc(url=url_target, table="log_user_behavior", properties=db_properties_target)
# df_product = spark.read.jdbc(url=url_source, table="product", properties=db_properties_source)
# df_store = spark.read.jdbc(url=url_source, table="store", properties=db_properties_source)

df = spark.read.csv('../data/data_mock/log_user_behavior.txt', header=True)
df2 = spark.read.csv('../data/data_mock/product.csv', header=True)
df3 = spark.read.csv('../data/data_mock/Stores.csv', header=True)

def answer_q4(df, df2, df3, store_id=None):
    try:
        # Filter only the 'click' actions
        df = df.filter(df['action'] == 'click')

        # Convert date to timestamp and create 'minute' column
        df = df.withColumn('date', F.to_timestamp('date'))
        df = df.withColumn('minute', F.date_format('date', 'yyyy-MM-dd HH:mm:00'))

        # Get the current hour
        this_hour = time.localtime().tm_hour

        # Filter records to include only the last hour
        df = df.filter(F.hour('date') == this_hour)

        #select df2 colums
        df2 = df2.select(['product_id', 'name', 'store_id'])

        # Group by 'button_product_id' and count views
        df = df.groupBy('button_product_id').agg(F.count('button_product_id').alias('view_count'))

        # Sort by 'view_count' in descending order and limit to top 5
        df = df.sort(F.desc('view_count')).limit(10)

        # Join with df2 to get product details including store_id
        df = df.join(df2, df['button_product_id'] == df2['product_id'], how='left')

        # only with store_id
        if store_id:
            df = df.filter(df['store_id'] == store_id)

        # Join with df3 to get store names
        df = df.join(df3, df['store_id'] == df3['store id'], how='left')

        # Select relevant columns
        df = df.select(df['button_product_id'].alias('PRODUCT ID'), 
                       df['view_count'].alias('VIEW COUNT'),
                       df2['name'].alias('PRODUCT NAME'),
                       df['store_id'].alias('STORE ID'),
                       df3['store_name'].alias('STORE NAME'))
        return df
    except Exception as e:
        print(f"Error: {e}")

df = answer_q4(df, df2, df3)