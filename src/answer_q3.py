# Conecta no banco de dados PostgreSQL Target e responde a pergunta 3
#  Número de usuários únicos visualizando cada produto por minuto

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"

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


def answer_q3(spark, store_id=None):
    # if table is false: return only the number of unique users
    df = spark.read.jdbc(url=url_target, table="user_behavior_log", properties=db_properties_target)
    df2 = spark.read.jdbc(url=url_source, table="product_data", properties=db_properties_source)
    try:
        if store_id:
            df = df.join(df2, df['button_product_id'] == df2['product_id'])
            df = df.filter(df['shop_id'] == store_id)
            
        df = df.filter(df['action'] == 'click')
        df = df.withColumn('date', F.to_timestamp('date'))
        #get min minute and max minute
        min_minute = df.agg({'date': 'min'}).collect()[0][0]
        max_minute = df.agg({'date': 'max'}).collect()[0][0]
        df = df.withColumn('minute', F.date_format('date', 'yyyy-MM-dd HH:mm:00'))
        
        df = df.select(['minute', 'button_product_id', 'user_author_id'])
        df = df.groupBy(['minute', 'button_product_id']).agg(F.countDistinct('user_author_id').alias('unique_users'))
        df = df.sort('minute')
        # divide by max minutes - min minutes
        number = df.withColumn('unique_users', F.col('unique_users')/(max_minute - min_minute).total_seconds()/60)
        # sum all unique users

        number = number.groupBy().agg(F.sum('unique_users').alias('unique_users'))
        return df, number
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Answer Q3") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()
    df, number = answer_q3(spark)
    df.show()
    print(number)
    number.show()