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

url = config['db_source_url']
db_properties = {
    "user": config['db_source_user'],
    "password": config['db_source_password'],
    "driver": "org.postgresql.Driver"
}



def answer_q6(spark, store_id = None):
    df = spark.read.jdbc(url=url, table="order_data", properties=db_properties)
    df2 = spark.read.jdbc(url=url, table="stock_data", properties=db_properties)

    try:
        # Rename columns in df2 for consistency
        df2 = df2.withColumnRenamed('product_id', 'ID PRODUTO')
        df2 = df2.withColumnRenamed('quantity', 'QUANTIDADE EM ESTOQUE')

        if store_id:
            df = df.filter(df['shop_id'] == store_id)
        # df.withColumnRenamed('quantity', 'quantity_bought')
        # Join df and df2 on product_id
        df = df.join(df2, df['product_id'] == df2['ID PRODUTO'])

        # Cast columns to appropriate types
        df = df.withColumn('quantity', df['quantity'].cast('int'))
        df = df.withColumn('QUANTIDADE EM ESTOQUE', df['QUANTIDADE EM ESTOQUE'].cast('int'))

        # Filter rows where quantity sold is greater than quantity in stock
        df = df.filter(df['quantity'] > df['QUANTIDADE EM ESTOQUE'])

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