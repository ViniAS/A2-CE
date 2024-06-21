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

url_processed = config['db_processed_url']
db_properties_processed = {
    "user": config['db_processed_user'],
    "password": config['db_processed_password'],
    "driver": "org.postgresql.Driver"
}

def answer_q4(spark, store_id=None):
    df = spark.read.jdbc(url=url_processed, table="pre_process_q4", properties=db_properties_processed)
    #columns 'shop_id', 'shop_name', 'product_name', 'action', 'date
    try:
        # Filter only the 'click' actions
        df = df.filter(df['action'] == 'click')
        if store_id:
            df = df.filter(df['shop_id'] == store_id)

        # Convert date to timestamp and create 'minute' column
        df = df.withColumn('date', F.to_timestamp('date'))
        df = df.withColumn('minute', F.date_format('date', 'yyyy-MM-dd HH:mm:00'))

        # Get the current hour
        this_hour = time.localtime().tm_hour

        # Filter records to include only the last hour
        df = df.filter(F.hour('date') == this_hour)
        df = df.groupBy('product_id', 'shop_id','name', 'shop_name').agg(F.count('*').alias('view_count'))
        # Select relevant columns
        df = df.select(['view_count', 'name', 'shop_name'])
        # Sort by view_count
        df = df.sort('view_count', ascending=False)
        return df
    
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