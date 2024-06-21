# Conecta nos Bancos de Dados PostgreSQL Source e Target e responde a pergunta 5
#  Mediana do número de vezes que um usuário visualiza um produto antes de 
# efetuar uma compra.

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import sys
from pyspark.sql.window import Window


# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"

# Load configuration from config.json
with open('src/config.json') as f:
    config = json.load(f)


url_processed = config['db_processed_url']
db_properties_processed = {
    "user": config['db_processed_user'],
    "password": config['db_processed_password'],
    "driver": "org.postgresql.Driver"
}

def answer_q5(spark, store_id = None):
    df = spark.read.jdbc(url=url_processed, table="pre_process_q5", properties=db_properties_processed)
    try:
        # Filter only the 'click' actions in df
        df = df.filter(df['action'] == 'click')
         #filter by store_id
        if store_id:
            df = df.filter(df['shop_id'] == store_id)

        # Convert date to timestamp and create 'minute' column in df
        df = df.withColumn('date', F.to_timestamp('date'))
        df = df.withColumn('minute', F.date_format('date', 'yyyy-MM-dd HH:mm:00'))

        df = df.withColumn('purchase_date', F.to_timestamp('purchase_date'))
        df = df.withColumn('minute_purchase', F.date_format('purchase_date', 'yyyy-MM-dd HH:mm:00'))

        # Filter views that happened before the purchase
        df = df.filter(F.col('minute') < F.col('minute_purchase'))
        # Count the number of views before each purchase
        views_before_purchase = df.groupBy('user_author_id', 'product_id', 'minute_purchase').agg(F.count('*').alias('views_before_purchase'))
        
        # Check if df is empty
        if views_before_purchase.rdd.isEmpty():
            return 0
        # Calculate the median of views_before_purchase
        median_views_before_purchase = views_before_purchase.approxQuantile('views_before_purchase', [0.5], 0.01)[0]

        return median_views_before_purchase
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Answer Q5") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()
    a = answer_q5(spark, store_id = None)
    print(a)