import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import os

# Configuration
NUM_FILES = 1000  # Number of files to process per execution
LOG_DIR = "data"
JDBC_DRIVER_PATH = "jdbc/postgresql-42.7.3.jar"
PATH_TO_LAST_PROCESSED_ID = 'json/last_processed_id_log.json'
CONFIG_PATH = 'src/config.json'

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

def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)

def process_files(spark, file_paths, schema, column_names):
    # Filter out files that do not exist
    print(schema)
    existing_file_paths = [fp for fp in file_paths if os.path.exists(fp)]
    len_existing = len(existing_file_paths) if existing_file_paths else 0
    if not existing_file_paths:
        return None, 0
    df = spark.read.csv(existing_file_paths, header=False, schema=schema)
    return df.toDF(*column_names), len_existing

def main():
    # Load configuration
    config = load_config(CONFIG_PATH)
    url = config['db_target_url']
    db_properties = {
        "user": config['db_target_user'],
        "password": config['db_target_password'],
        "driver": "org.postgresql.Driver"
    }
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Extract Log") \
        .config("spark.jars", JDBC_DRIVER_PATH) \
        .getOrCreate()

    last_ids = get_last_processed_ids(PATH_TO_LAST_PROCESSED_ID)
    last_id_user_behavior = last_ids.get('user_behavior_log', 0)
    last_id_failure = last_ids.get('log_failure', 0)
    last_id_debug = last_ids.get('log_debug', 0)
    last_id_audit = last_ids.get('log_audit', 0)

    user_behavior_files = [os.path.join(LOG_DIR, 'behaviour', f'log_{i}.txt') for i in range(last_id_user_behavior + 1, last_id_user_behavior + NUM_FILES)]
    failure_files = [os.path.join(LOG_DIR, 'fail', f'log_{i}.txt') for i in range(last_id_failure + 1, last_id_failure + NUM_FILES)]
    debug_files = [os.path.join(LOG_DIR, 'debug', f'log_{i}.txt') for i in range(last_id_debug + 1, last_id_debug + NUM_FILES)]
    audit_files = [os.path.join(LOG_DIR, 'audit', f'log_{i}.txt') for i in range(last_id_audit + 1, last_id_audit + NUM_FILES)]

    # Define schemas
    user_behavior_schema = StructType([
        StructField("user_author_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("button_product_id", IntegerType(), True),
        StructField("date", TimestampType(), True)
    ])
    failure_schema = StructType([
        StructField("component", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("message", StringType(), True),
        StructField("text_content", StringType(), True),
        StructField("date", TimestampType(), True)
    ])
    debug_schema = StructType([
        StructField("message", StringType(), True),
        StructField("text_content", StringType(), True),
        StructField("date", TimestampType(), True)
    ])
    audit_schema = StructType([
        StructField("user_author_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("action_description", StringType(), True),
        StructField("text_content", StringType(), True),
        StructField("date", TimestampType(), True)
    ])

    # Process files
    log_user_behavior, len_existing_behavior = process_files(spark, user_behavior_files, user_behavior_schema, ["user_author_id", "action", "button_product_id", "date"])
    log_failure, len_existing_failure = process_files(spark, failure_files, failure_schema, ["component", "severity", "message", "text_content", "date"])
    log_debug, len_existing_debug = process_files(spark, debug_files, debug_schema, ["message", "text_content", "date"])
    log_audit, len_existing_audit = process_files(spark, audit_files, audit_schema, ["user_author_id", "action", "action_description", "text_content", "date"])

    if log_user_behavior:
        log_user_behavior.write.jdbc(url=url, table="user_behavior_log", mode="append", properties=db_properties)
        update_last_processed_id(PATH_TO_LAST_PROCESSED_ID, 'user_behavior_log', last_id_user_behavior + len_existing_behavior)

    if log_failure:
        log_failure.write.jdbc(url=url, table="log_failure", mode="append", properties=db_properties)
        update_last_processed_id(PATH_TO_LAST_PROCESSED_ID, 'log_failure', last_id_failure + len_existing_failure)

    if log_debug:
        log_debug.write.jdbc(url=url, table="log_debug", mode="append", properties=db_properties)
        update_last_processed_id(PATH_TO_LAST_PROCESSED_ID, 'log_debug', last_id_debug + len_existing_debug)

    if log_audit:
        log_audit.write.jdbc(url=url, table="log_audit", mode="append", properties=db_properties)
        update_last_processed_id(PATH_TO_LAST_PROCESSED_ID, 'log_audit', last_id_audit + len_existing_audit)

    spark.stop()

if __name__ == '__main__':
    main()