# Usa o Spark para acessar um webhook e extrair os dados dele e enviar para uma queue RabbitMQ

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# TODO: Conexão com o Webhook