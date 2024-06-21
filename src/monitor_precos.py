import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import sys
from pyspark.sql.window import Window


# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "/usr/share/java/postgresql-42.2.23.jar"

# Load configuration from config.json
with open('src/config.json') as f:
    config = json.load(f)

url_processed = config['db_processed_url']
db_properties_processed = {
    "user": config['db_processed_user'],
    "password": config['db_processed_password'],
    "driver": "org.postgresql.Driver"
}

def monitor(spark, periodo_meses = 24, porcentagem_abaixo_media= 0):
    df = spark.read.jdbc(url=url_processed, table="pre_process_monitor", properties=db_properties_processed)
    try:
        # Convert purchase_date to timestamp and create 'minute_purchase' column in df
        df = df.withColumn('purchase_date', F.to_timestamp('purchase_date'))
        df = df.withColumn('month', F.date_format('purchase_date', 'yyyy-MM'))
        df = df.withColumn('year', F.date_format('purchase_date', 'yyyy'))
        # filter the last 24 months
        df = df.filter(F.col('purchase_date') > F.add_months(F.current_date(), -periodo_meses))
        df = df.withColumn('price', df['price'].cast('float'))
        window = Window.partitionBy('product_id')
        df = df.withColumn('mean_price', F.mean('price').over(window))
        df = df.withColumn('below_mean', F.col('price') < F.col('mean_price')*(1 - porcentagem_abaixo_media/100))
        # column with the comparable discount
        df = df.withColumn('discount', F.col('mean_price') - F.col('price'))
        df = df.filter(F.col('below_mean'))
        # sort by the product most below the mean price
        df = df.sort('discount', ascending=False)
        df = df.select(['name', 'price', 'mean_price', 'discount'])
        return df
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Monitor") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()
    df = monitor(spark, 24, 10)
    df.show()
    spark.stop()
        