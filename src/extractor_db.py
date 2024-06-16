# Usa o Spark para acessar um banco de dados PostgreSQL e extrair os dados dele e enviar para uma queue RabbitMQ

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json

with open('config.json') as f:
    config = json.load(f)

# Path do driver JDBC do PostgreSQL
jdbc_driver_path = "../jdbc/postgresql-42.7.3.jar"

# Propriedades de conexão com o banco de dados
db_properties = {
    "user": config['db_user'],
    "password": config['db_password'],
    "driver": "org.postgresql.Driver"
}

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Extract PostgreSQL") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

# Lê os dados de uma tabela PostgreSQL
db_df = spark.read.jdbc(url=config['db_url'], table="your_table_name", properties=db_properties)

# Printa os dados 
db_df.show()

# TODO: Enviar os dados para uma fila RabbitMQ