# Usa o Spark para acessar um diretório (s3 ou local) e extrair os arquivos de log dele e enviar para o 
# banco de dados PostgreSQL Target

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import input_file_name


NUM_FILES = 1000 # Número de arquivos a serem processados por execução

# Path para o diretório de logs
log_dir = "data"
# s3_log_dir = "s3://bucket-name/logs"

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Extract Log") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

path_to_last_processed_id = 'json/last_processed_id_log.json'

def get_last_processed_ids(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {}
    
def update_last_processed_id(file_path, table_name, last_id):
    ids = get_last_processed_ids(file_path)
    ids[table_name] = last_id
    with open(file_path, 'w') as file:
        json.dump(ids, file)

# Acha o último id processado
last_ids = get_last_processed_ids(path_to_last_processed_id)
last_id_user_behavior = last_ids.get('user_behavior_log', 0)
last_id_failure = last_ids.get('log_failure', 0)
last_id_debug = last_ids.get('log_debug', 0)
last_id_audit = last_ids.get('log_audit', 0)

user_behavior_files_pattern = f"{log_dir}/behaviour/log_{{id}}.txt"
user_behavior_file_paths = [user_behavior_files_pattern.format(id=i) for i in range(last_id_user_behavior + 1, last_id_user_behavior + NUM_FILES)]  # Adjust the range as needed

failure_files_pattern = f"{log_dir}/fail/log_{{id}}.txt"
failure_file_paths = [failure_files_pattern.format(id=i) for i in range(last_id_failure + 1, last_id_failure + NUM_FILES)]  # Adjust the range as needed

debug_files_pattern = f"{log_dir}/debug/log_{{id}}.txt"
debug_file_paths = [debug_files_pattern.format(id=i) for i in range(last_id_debug + 1, last_id_debug + NUM_FILES)]  # Adjust the range as needed

audit_files_pattern = f"{log_dir}/audit/log_{{id}}.txt"
audit_file_paths = [audit_files_pattern.format(id=i) for i in range(last_id_audit + 1, last_id_audit + NUM_FILES)]  # Adjust the range as needed


# Define schema based on your database requirements
schema = StructType([
    StructField("user_author_id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("button_product_id", IntegerType(), True),
    StructField("stimulus", StringType(), True),
    StructField("component", StringType(), True),
    StructField("text_content", StringType(), True),
    StructField("date", TimestampType(), True),
])
# ler a partir do diretório de logs a partir do arquivo "log_{last_id_user_behavior}.txt" em diante
log_user_behavior = spark.read.csv(user_behavior_file_paths, header=False, schema =schema)

column_names = ["user_author_id","action", "button_product_id",
                "stimulus", "component", "text_content", "date"]
log_user_behavior = log_user_behavior.toDF(*column_names)

schema = StructType([
    StructField("message", StringType(), True),
    StructField("text_content", StringType(), True),
    StructField("date", TimestampType(), True),
])
log_debug = spark.read.csv(debug_file_paths, header=False, schema =schema)
column_names = ["message", "text_content", "date"]
log_debug = log_debug.toDF(*column_names)

schema = StructType([
    StructField("component", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("message", StringType(), True),
    StructField("text_content", StringType(), True),
    StructField("date", TimestampType(), True),
])
log_failure = spark.read.csv(failure_file_paths, header=False, schema =schema)
column_names = ["component", "severity", "message", "text_content", "date"]
log_failure = log_failure.toDF(*column_names)

schema = StructType([
    StructField("user_author_id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("action_description", StringType(), True),
    StructField("text_content", StringType(), True),
    StructField("date", TimestampType(), True),
])
log_audit = spark.read.csv(audit_file_paths, header=False, schema =schema)
column_names = ["user_author_id", "action", "action_description", 
                "text_content", "date"]
log_audit = log_audit.toDF(*column_names)


update_last_processed_id(path_to_last_processed_id, 'user_behavior_log', last_id_user_behavior + NUM_FILES)
update_last_processed_id(path_to_last_processed_id, 'log_failure', last_id_failure + NUM_FILES)
update_last_processed_id(path_to_last_processed_id, 'log_debug', last_id_debug + NUM_FILES)
update_last_processed_id(path_to_last_processed_id, 'log_audit', last_id_audit + NUM_FILES)

# select columns to write to the database
log_user_behavior = log_user_behavior.select(["user_author_id", "action", "button_product_id", "date"])

# Load database properties from config.json
with open('src/config.json') as f:
    config = json.load(f)

url = config['db_target_url']
db_properties = {
    "user": config['db_target_user'],
    "password": config['db_target_password'],
    "driver": "org.postgresql.Driver"
}

#writes the data to a PostgreSQL database
log_user_behavior.write.jdbc(url=url, table="user_behavior_log", mode="append", properties=db_properties)
log_debug.write.jdbc(url=url, table="log_debug", mode="append", properties=db_properties)
log_failure.write.jdbc(url=url, table="log_failure", mode="append", properties=db_properties)
log_audit.write.jdbc(url=url, table="log_audit", mode="append", properties=db_properties)

spark.stop()