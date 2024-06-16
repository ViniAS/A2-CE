# Usa o Spark para acessar um diretório (s3 ou local) e extrair os arquivos de log dele e enviar para uma queue RabbitMQ

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Path para o diretório de logs
log_dir = "../data/logs"
# s3_log_dir = "s3://bucket-name/logs"

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Extract Log") \
    .getOrCreate()

# Lê os dados de log
log_df = spark.read.text(log_dir)

# printa os dados
log_df.show()

# TODO: Implementar o código para enviar os dados para uma fila RabbitMQ