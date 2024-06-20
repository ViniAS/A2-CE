# Conecta nos Bancos de Dados PostgreSQL Source e Target e responde a pergunta 4
# Ranking de produtos mais visualizados na última hora

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import time

# Load configuration from config.json
with open('src/config.json') as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"

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
        df2 = df2.select(['product_id', 'name', 'shop_id'])

        # Filter only the 'click' actions
        df = df.filter(df['action'] == 'click')
        if store_id:
            df2 = df2.filter(df2['shop_id'] == store_id)
            df3 = df3.filter(df3['shop_id'] == store_id)

        # Convert date to timestamp and create 'minute' column
        df = df.withColumn('date', F.to_timestamp('date'))
        df = df.withColumn('minute', F.date_format('date', 'yyyy-MM-dd HH:mm:00'))
        df = df.select(['minute', 'button_product_id'])

        # Get the current hour
        this_hour = time.localtime().tm_hour

        # Filter records to include only the last hour
        df = df.filter(F.hour('date') == this_hour)

        # Group by 'button_product_id' and count views
        df = df.groupBy('button_product_id').agg(F.count('button_product_id').alias('view_count'))

        # Join with df2 to get product details including store_id
        df2 = df2.join(df, df2['product_id'] == df['button_product_id'], how='inner')
        
        # Join with df3 to get store names
        df2 = df2.join(df3, df2['shop_id'] == df3['shop_id'], how='inner')
        # Select relevant columns
        df2 = df2.select(df['view_count'].alias('VIEW COUNT'),
                       df2['name'].alias('PRODUCT NAME'),
                       df3['shop_name'].alias('STORE NAME'))
        # Sort by view_count
        df2 = df2.sort('VIEW COUNT', ascending=False)
        return df2
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Answer Q4") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()
    df = answer_q4(spark)
    df.show()
    spark.stop()