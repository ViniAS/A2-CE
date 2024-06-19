import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import sys
from pyspark.sql.window import Window


# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("monitor") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

# Load configuration from config.json
with open('src/config.json') as f:
    config = json.load(f)


url_source = config['db_source_url']
db_properties_source = {
    "user": config['db_source_user'],
    "password": config['db_source_password'],
    "driver": "org.postgresql.Driver"
}

def monitor(spark, periodo_meses = 24, porcentagem_abaixo_media= 0):
    df = spark.read.jdbc(url=url_source, table="order_data", properties=db_properties_source)
    df2 = spark.read.jdbc(url=url_source, table="product_data", properties=db_properties_source)

    try:
        # Convert purchase_date to timestamp and create 'minute_purchase' column in df
        df2 = df2.select(['product_id', 'name'])
        df = df.select(['product_id', 'price', 'purchase_date'])
        df = df.withColumn('purchase_date', F.to_timestamp('purchase_date'))
        df = df.withColumn('month', F.date_format('purchase_date', 'yyyy-MM'))
        df = df.withColumn('year', F.date_format('purchase_date', 'yyyy'))
        # filter the last 24 months
        df = df.filter(F.col('purchase_date') > F.add_months(F.current_date(), -periodo_meses))
        df = df.withColumn('price', df['price'].cast('float'))
        # df = df.groupBy('product_id', 'year', 'month').agg(F.mean('price').alias('mean_price'))
        # new column with the mean price of the product in all of extended period
        window = Window.partitionBy('product_id')
        df = df.withColumn('mean_price', F.mean('price').over(window))
        df = df.withColumn('below_mean', F.col('price') < F.col('mean_price')*(1 - porcentagem_abaixo_media/100))
        # column with the comparable discount
        df = df.withColumn('discount', F.col('mean_price') - F.col('price'))
        df = df.filter(F.col('below_mean'))
        # sort by the product most below the mean price
        df = df.sort('discount', ascending=False)
        df = df.select(['product_id', 'price', 'mean_price', 'discount'])
        df = df.join(df2, df['product_id'] == df2['product_id'])

        return df
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    

if __name__ == "__main__":
    monitor(spark).show()
    spark.stop()
        