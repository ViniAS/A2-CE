from celery import Celery
import json
from pyspark.sql import SparkSession

app = Celery('tasks', broker='pyamqp://guest@localhost//')

with open('config.json') as f:
    config = json.load(f)

# Path do driver JDBC do PostgreSQL
jdbc_driver_path = "../jdbc/postgresql-42.7.3.jar"

# Propriedades de conexão com o banco de dados
db_properties = {
    "user": config['db_user'],
    "password": config['db_password'],
    "driver": "org.postgresql.Driver",
    "url": config['db_url']
}



@app.task
def store_user_behavior(message):
    # Cria uma sessão Spark
    spark = SparkSession.builder \
        .appName("Store Log") \
        .config("spark.jars", jdbc_driver_path) \
        .config("spark.ui.port", "4050") \
        .getOrCreate()

    df = spark.createDataFrame([message])

    df.write.jdbc(url=db_properties['url'], table="log_user_behavior",
                  mode="append", properties=db_properties)

    spark.stop()


