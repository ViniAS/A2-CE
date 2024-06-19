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

# Cria uma sessão Spark
# spark = SparkSession.builder \
#     .appName("Answer Q5") \
#     .config("spark.jars", jdbc_driver_path) \
#     .getOrCreate()

# Load configuration from config.json
with open('src/config.json') as f:
    config = json.load(f)


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

def answer_q5(spark, store_id = None):
    df = spark.read.jdbc(url=url_target, table="user_behavior_log", properties=db_properties_target)
    df2 = spark.read.jdbc(url=url_source, table="order_data", properties=db_properties_source)


    try:
        # Filter only the 'click' actions in df
        df = df.filter(df['action'] == 'click')
         #filter by store_id
        if store_id:
            df2 = df2.filter(df2['shop_id'] == store_id)


        # Convert date to timestamp and create 'minute' column in df
        df = df.withColumn('date', F.to_timestamp('date'))
        df = df.withColumn('minute', F.date_format('date', 'yyyy-MM-dd HH:mm:00'))
        
        # Select relevant columns and rename them for consistency
        df = df.select(['minute', 'button_product_id', 'user_author_id'])
        df = df.withColumnRenamed('button_product_id', 'product_id')
        df = df.withColumnRenamed('user_author_id', 'user_id')


        # Convert purchase_date to timestamp and create 'minute_purchase' column in df2
        df2 = df2.withColumn('purchase_date', F.to_timestamp('purchase_date'))
        df2 = df2.withColumn('minute_purchase', F.date_format('purchase_date', 'yyyy-MM-dd HH:mm:00'))

        # Select relevant columns from df2
        df2 = df2.select(['minute_purchase', 'product_id', 'user_id'])
        # Join the dataframes on user_id and product_id
        joined_df = df.join(df2, ['user_id', 'product_id'])
        # Filter views that happened before the purchase
        joined_df = joined_df.filter(F.col('minute') < F.col('minute_purchase'))
        # Count the number of views before each purchase
        views_before_purchase = joined_df.groupBy('user_id', 'product_id', 
                                                  'minute_purchase').agg(F.count('*').alias('views_before_purchase'))
        
        # Check if df is empty
        if views_before_purchase.rdd.isEmpty():
            return 0
        # Calculate the median of views_before_purchase
        median_views_before_purchase = views_before_purchase.approxQuantile('views_before_purchase', [0.5], 0.01)[0]

        return median_views_before_purchase
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
# if __name__ == "__main__":
#     a = answer_q5(spark, store_id = None)
#     print(type(a))
#     print(a)