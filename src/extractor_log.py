# Usa o Spark para acessar um diretório (s3 ou local) e extrair os arquivos de log dele e enviar para o 
# banco de dados PostgreSQL Target

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json

# Path para o diretório de logs
log_dir = "../data/logs"
# s3_log_dir = "s3://bucket-name/logs"

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Extract Log") \
    .getOrCreate()

# Lê os dados de log
log_user_behavior = spark.read.text(log_dir+"/user_behavior", header=["user_author_id", "action", 
                                                                      "button_product_id", "stimulus", 
                                                                      "component", "text_content", "date"])

log_debug = spark.read.text(log_dir+"/debug", header=["message", "text_content", "date"])

log_failure = spark.read.text(log_dir+"/failure", header=["component", "severity", "message", 
                                                          "text_content", "date"])

log_audit = spark.read.text(log_dir+"/audit", header=["user_author_id", "action", "action_description", 
                                                      "text_content", "date"])

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

#writes the data to a PostgreSQL database
log_user_behavior.write.jdbc(url=url, table="log_user_behavior", mode="overwrite", properties=db_properties)
log_debug.write.jdbc(url=url, table="log_debug", mode="overwrite", properties=db_properties)
log_failure.write.jdbc(url=url, table="log_failure", mode="overwrite", properties=db_properties)
log_audit.write.jdbc(url=url, table="log_audit", mode="overwrite", properties=db_properties)