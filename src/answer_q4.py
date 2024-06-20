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

# Load configuration from config.json
with open('src/config.json') as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "/usr/share/java/postgresql-42.2.23.jar"

# # Cria uma sessão Spark


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




def answer_q4(spark, store_id=None):
    df= spark.read.jdbc(url=url_target, table="user_behavior_log", properties=db_properties_target)
    df2 = spark.read.jdbc(url=url_source, table="product_data", properties=db_properties_source)
    df3= spark.read.jdbc(url=url_source, table="shop_data", properties=db_properties_source)
    try:
        # Filter only the 'click' actions
        df = df.filter(df['action'] == 'click')
        if store_id:
            df2 = df2.filter(df2['shop_id'] == store_id)
            df3 = df3.filter(df3['shop_id'] == store_id)

        df.show()
        # Convert date to timestamp and create 'minute' column
        df = df.withColumn('date', F.to_timestamp('date'))
        df = df.withColumn('minute', F.date_format('date', 'yyyy-MM-dd HH:mm:00'))

        # Get the current hour
        this_hour = time.localtime().tm_hour

        # Filter records to include only the last hour
        df = df.filter(F.hour('date') == this_hour)
        df.show()

        #select df2 colums
        df2 = df2.select(['product_id', 'name', 'shop_id'])
        # Group by 'button_product_id' and count views
        df = df.groupBy('button_product_id').agg(F.count('button_product_id').alias('view_count'))
        df.show()
        # Sort by 'view_count' in descending order and limit to top 5
        df = df.sort(F.desc('view_count')).limit(10)

        # Join with df2 to get product details including store_id
        df2 = df2.join(df, df2['product_id'] == df['button_product_id'], how='inner')
        # only with store_id
        
        #rename store id df3
        df3 = df3.withColumnRenamed('shop_id', 'shop id')
        # Join with df3 to get store names
        df2 = df2.join(df3, df2['shop_id'] == df3['shop id'], how='inner')
        # Select relevant columns
        df2 = df2.select(df['button_product_id'].alias('PRODUCT ID'), 
                       df['view_count'].alias('VIEW COUNT'),
                       df2['name'].alias('PRODUCT NAME'),
                       df2['shop_id'].alias('STORE ID'),
                       df3['shop_name'].alias('STORE NAME'))
        
        df2.sort('VIEW COUNT', ascending=False)
        df2.show()
        return df2
    except Exception as e:
        print(f"Error: {e}")
