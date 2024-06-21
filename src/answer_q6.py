# Conecta no banco de dados PostgreSQL Source e responde a pergunta 6
# Número de produtos vendidos sem disponibilidade no estoque.
# return table format:
# Total Excess Sales
# 2075

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json

# Load configuration from config.json
with open('src/config.json') as f:
    config = json.load(f)

# # Path to the PostgreSQL JDBC driver
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"

url_processed = config['db_processed_url']
db_properties_processed = {
    "user": config['db_processed_user'],
    "password": config['db_processed_password'],
    "driver": "org.postgresql.Driver"
}

# order_data.purchase_date, stock_data.stock_quantity, order_data.quantity, order_data.shop_id

def answer_q6(spark, store_id = None):
    df = spark.read.jdbc(url=url_processed, table="pre_process_q6", properties=db_properties_processed)
    try:
        if store_id:
            df = df.filter(df['shop_id'] == store_id)
        # Cast columns to appropriate types
        df = df.withColumn('quantity', df['quantity'].cast('int'))
        df = df.withColumn('stock_quantity', df['stock_quantity'].cast('int'))
        # Filter rows where quantity sold is greater than quantity in stock
        df = df.filter(df['quantity'] > df['stock_quantity'])
        df = df.withColumn('quantity', df['quantity'] - df['stock_quantity'])
        # Sum the quantities of products sold without stock availability
        result_df = df.groupBy().agg(F.sum('quantity').alias('Total Excess Sales'))
        return result_df

    except Exception as e:
        print(f"Error: {e}")
        return None
    
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Answer Q6") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()
    df = answer_q6(spark)
    df.show()
    spark.stop()