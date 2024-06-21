# Conecta no banco de dados PostgreSQL Source e responde a pergunta 2
# Valor faturado por minuto.

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json

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


def answer_q2(spark, store_id=None):
    df = spark.read.jdbc(url=url_processed, table="pre_process_q2", properties=db_properties_processed)
    try:
        if store_id:
            df = df.filter(F.col('shop_id') == store_id)

        df = df.withColumn('purchase_date', F.to_timestamp('purchase_date'))
        df = df.withColumn('minute',F.date_format('purchase_date', 'yyyy-MM-dd HH:mm:00'))

        df = df.withColumn('revenue', df['quantity'] * df['price'])
        df = df.groupBy('minute').agg(F.sum('revenue').alias('revenue_per_minute'))
        df = df.sort('minute')
        lines_count = df.count()
        number = df.withColumn('revenue_per_minute', F.col('revenue_per_minute')/lines_count)
        number = number.groupBy().agg(F.sum('revenue_per_minute').alias('revenue_per_minute'))
        return df, number
    except Exception as e:
        print(f"Error: {e}")

# create table q2 (minute timestamp, revenue_per_minute float);

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Answer Q2") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()
    df, number = answer_q2(spark)
    df.show()
    print(number)
    number.show()