# Conecta nos Bancos de Dados PostgreSQL Source e Target e responde a pergunta 5
#  Mediana do número de vezes que um usuário visualiza um produto antes de 
# efetuar uma compra.

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import sys
from pyspark.sql.window import Window

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Answer Q5") \
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
# df_order = spark.read.jdbc(url=url_source, table="order", properties=db_properties_source)


df_behavior = spark.read.csv('../data/data_mock/log_user_behavior.txt', header=True)
df_order = spark.read.csv('../data/data_mock/order.csv', header=True)

def answer_q5(df, df2, store_id = None):
    try:
        # Filter only the 'click' actions in df
        df = df.filter(df['action'] == 'click')

        # Convert date to timestamp and create 'minute' column in df
        df = df.withColumn('date', F.to_timestamp('date'))
        df = df.withColumn('minute', F.date_format('date', 'yyyy-MM-dd HH:mm:00'))
        
        # Select relevant columns and rename them for consistency
        df = df.select(['minute', 'button_product_id', 'user_author_id'])
        df = df.withColumnRenamed('button_product_id', 'product_id')
        df = df.withColumnRenamed('user_author_id', 'user_id')

        #filter by store_id
        if store_id:
            df2 = df2.filter(df2['store_id'] == store_id)

        # Convert purchase_date to timestamp and create 'minute_purchase' column in df2
        df2 = df2.withColumn('purchase_date', F.to_timestamp('purchase_date'))
        df2 = df2.withColumn('minute_purchase', F.date_format('purchase_date', 'yyyy-MM-dd HH:mm:00'))

        # Select relevant columns from df2
        df2 = df2.select(['minute_purchase', 'product_id', 'user_id'])

        # Join the dataframes on user_id and product_id
        joined_df = df.join(df2, ['user_id', 'product_id'])

        # Filter views that happened before the purchase
        joined_df = joined_df.filter(F.col('minute') < F.col('minute_purchase'))
        joined_df.show()
        # Count the number of views before each purchase
        views_before_purchase = joined_df.groupBy('user_id', 'product_id', 
                                                  'minute_purchase').agg(F.count('*').alias('views_before_purchase'))
        views_before_purchase.show()
        # Calculate the median of views_before_purchase
        median_views_before_purchase = views_before_purchase.approxQuantile('views_before_purchase', [0.5], 0.01)[0]

        return median_views_before_purchase
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

df = answer_q5(df_behavior,df_order)

print(df)