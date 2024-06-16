# Conecta em uma queue RabbitMQ e processa os dados recebidos para responder a pergunta 1:
# Número de produtos comprados por minuto
# return table format: 
# PURCHASE DATE,QUANTITY
# 2024-06-08 12:04:00,6
# 2024-06-08 12:05:00,1
# 2024-06-08 12:06:00,4
# 2024-06-08 12:07:00,4
# 2024-06-08 12:08:00,8
# 2024-06-08 12:09:00,4
# 2024-06-08 12:10:00,6
# 2024-06-08 12:11:00,3
# 2024-06-08 12:12:00,5
# 2024-06-08 12:13:00,8
# 2024-06-08 12:14:00,7
# 2024-06-08 12:15:00,9
# 2024-06-08 12:16:00,9
# 2024-06-08 12:17:00,2
# 2024-06-08 12:18:00,7

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Answer Q1") \
    .getOrCreate()

# TODO: Conexão com a queue Redis

df = spark.read.csv('../data/data_mock/order.csv', header=True)

# Calcula quantas compras por minuto foram realizadas
def answer_q1(df):
    try:
        df = df.withColumn(' DATA DE COMPRA', F.to_timestamp(' DATA DE COMPRA'))
        df = df.withColumn('minute',
                           F.concat(F.year(' DATA DE COMPRA'), F.lit('-'), F.month(' DATA DE COMPRA'), F.lit('-'), F.dayofmonth(' DATA DE COMPRA'), F.lit(' '), F.hour(' DATA DE COMPRA'), F.lit(':'), F.minute(' DATA DE COMPRA'), F.lit(':00')))
        
        df = df.select(['minute', ' QUANTIDADE'])
        df = df.groupBy('minute').agg(F.sum(' QUANTIDADE').alias('quantity'))
        # ordena por minuto crescente
        df = df.sort('minute')


        df.show()
        print((df.count(), len(df.columns)))
        return df
    except Exception as e:
        print(f"Error: {e}")

df = answer_q1(df)