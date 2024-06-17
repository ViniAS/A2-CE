# Conecta no banco de dados PostgreSQL Target e responde a pergunta 3
#  Número de usuários únicos visualizando cada produto por minuto
# return table format:
# VIEW DATE,PRODUCT ID,UNIQUE USERS
# 2024-06-08 12:04:00,69,1
# 2024-06-08 12:05:00,21,1
# 2024-06-08 12:06:00,3,1

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Answer Q3") \
    .getOrCreate()

# Load configuration from config.json
with open('config.json') as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "../jdbc/postgresql-42.7.3.jar"

url = config['db_target_url']
db_properties = {
    "user": config['db_target_user'],
    "password": config['db_target_password'],
    "driver": "org.postgresql.Driver"
}

# df = spark.read.jdbc(url=url, table="log_user_behavior", properties=db_properties)

df = spark.read.csv('../data/data_mock/log_user_behavior.txt', header=True)

def answer_q3(df):
    try:
        df = df.filter(df['action'] == 'click')
        df = df.withColumn('date', F.to_timestamp('date'))
        df = df.withColumn('minute', F.concat(F.year('date'), F.lit('-'), F.month('date'), F.lit('-'), 
                            F.dayofmonth('date'), F.lit(' '), F.hour('date'), F.lit(':'), F.minute('date'), 
                            F.lit(':00')))
        
        df = df.select(['minute', 'button_product_id', 'user_author_id'])
        df = df.groupBy(['minute', 'button_product_id']).agg(F.countDistinct('user_author_id').alias('unique_users'))
        df = df.sort('minute')

        print((df.count(), len(df.columns)))
        df.show()
        return df
    except Exception as e:
        print(f"Error: {e}")

df = answer_q3(df)