# Conecta em uma queue RabbitMQ e processa os dados recebidos para responder a pergunta 1

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Answer Q1") \
    .getOrCreate()

# TODO: Conexão com a queue RabbitMQ

df = spark.read.csv('../data/order.csv', header=True)

# Calcula quantas compras por minuto foram realizadas
def answer_q1(df):
    try:
        # quantity of purchases per minute
        df = df.withColumn(' DATA DE COMPRA', F.to_timestamp(' DATA DE COMPRA'))
        df = df.withColumn('minute', F.minute(' DATA DE COMPRA'))
        df = df.select(['minute', ' QUANTIDADE'])

        # numeror de minutos totais é a diferença entre o maior e o menor minuto
        num_minutes = df.agg(F.max('minute') - F.min('minute')).collect()[0][0]
        #  numero de compras é a soma da quantidade de compras
        num_purchases = df.agg(F.sum(' QUANTIDADE')).collect()[0][0]
        purchases_per_minute = num_purchases / num_minutes
        print(f'Número de compras por minuto: {purchases_per_minute}')
    except Exception as e:
        print(f"Error: {e}")
