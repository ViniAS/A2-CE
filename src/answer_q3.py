# Conecta em uma queue RabbitMQ e processa os dados recebidos para responder a pergunta 3
#  Número de usuários únicos visualizando cada produto por minuto
# return table format:
# VIEW DATE,PRODUCT ID,UNIQUE USERS
# 2024-06-08 12:04:00,69,1
# 2024-06-08 12:05:00,21,1
# 2024-06-08 12:06:00,3,1
# 2024-06-08 12:07:00,20,1
# 2024-06-08 12:08:00,3,1
# 2024-06-08 12:09:00,48,1
# 2024-06-08 12:10:00,95,1
# 2024-06-08 12:11:00,98,1
# 2024-06-08 12:12:00,64,1
# 2024-06-08 12:13:00,10,1
# on the log_user_behavior.txt we have the user_id, the action, if the action is 'view' we have the product_id of the product that the user viewed


import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Answer Q3") \
    .getOrCreate()

# TODO: Conexão com a queue Redis

df = spark.read.csv('../data/data_mock/log_user_behavior.txt', header=True)

def answer_q3(df):
    try:
        df = df.filter(df['action'] == 'click')
        df = df.withColumn('date', F.to_timestamp('date'))
        df = df.withColumn('minute', F.concat(F.year('date'), F.lit('-'), F.month('date'), F.lit('-'), F.dayofmonth('date'), F.lit(' '), F.hour('date'), F.lit(':'), F.minute('date'), F.lit(':00')))
        df = df.select(['minute', 'button_product_id', 'user_author_id'])
        df = df.groupBy(['minute', 'button_product_id']).agg(F.countDistinct('user_author_id').alias('unique_users'))
        df = df.sort('minute')

        print((df.count(), len(df.columns)))
        df.show()
        return df
    except Exception as e:
        print(f"Error: {e}")

df = answer_q3(df)