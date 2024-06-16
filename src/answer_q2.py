# Conecta em uma queue RabbitMQ e processa os dados recebidos para responder a pergunta 2
#  Valor faturado por minuto.
# return table format:
# REVENUE DATE,REVENUE
# 2024-06-08 10:28:00,802.0589763285572
# 2024-06-08 10:29:00,804.3087244286917
# 2024-06-08 10:30:00,989.1108616859157
# 2024-06-08 10:31:00,275.15581652525634
# 2024-06-08 10:32:00,509.38137349244215
# 2024-06-08 10:33:00,325.31568424683064
# 2024-06-08 10:34:00,518.7616409416386
# 2024-06-08 10:35:00,479.820516715781
# 2024-06-08 10:36:00,333.213150933936
# 2024-06-08 10:37:00,535.6852179700197
# 2024-06-08 10:38:00,347.98901366453254
# 2024-06-08 10:39:00,607.8186187917331
# 2024-06-08 10:40:00,919.5124539659778
# 2024-06-08 10:41:00,987.1297164058947
# 2024-06-08 10:42:00,867.0074324556414
# 2024-06-08 10:43:00,688.0790441866267
# 2024-06-08 10:44:00,796.8425427371656
# orders of the products are in order.csv, and the price of each product is in product.csv
# in order we have the product_id and quantity, and in product we have the product_id and price

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Answer Q2") \
    .getOrCreate()

# TODO: Conexão com a queue Redis

df = spark.read.csv('../data/data_mock/order.csv', header=True)
df2 = spark.read.csv('../data/data_mock/product.csv', header=True)

def answer_q2(df, df2):
    try:
        df = df.join(df2, df[' ID PRODUTO'] == df2['ID'])
        df = df.withColumn(' QUANTIDADE', df[' QUANTIDADE'].cast('int'))
        df = df.withColumn(' PREÇO', df[' PREÇO'].cast('float'))
        df = df.withColumn(' DATA DE COMPRA', F.to_timestamp(' DATA DE COMPRA'))
        df = df.withColumn('minute',
                           F.concat(F.year(' DATA DE COMPRA'), F.lit('-'), F.month(' DATA DE COMPRA'), F.lit('-'), F.dayofmonth(' DATA DE COMPRA'), F.lit(' '), F.hour(' DATA DE COMPRA'), F.lit(':'), F.minute(' DATA DE COMPRA'), F.lit(':00')))
        df = df.select(['minute', ' QUANTIDADE', ' PREÇO'])
        df = df.withColumn('revenue', df[' QUANTIDADE'] * df[' PREÇO'])
        df = df.groupBy('minute').agg(F.sum('revenue').alias('revenue_per_minute'))

        df.show()
        print((df.count(), len(df.columns)))
        return df
    except Exception as e:
        print(f"Error: {e}")

df = answer_q2(df, df2)