# Pega colunas do target_db e faz operações de join e deixar a tabela pronta para ser acessada pela pergunta 1

from pyspark.sql import SparkSession
import json

# Load configuration from config.json
with open('src/config.json') as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"

url_target = config['db_target_url']
db_properties_target = {
    "user": config['db_target_user'],
    "password": config['db_target_password'],
    "driver": "org.postgresql.Driver"
}

url_processed = config['db_processed_url']
db_properties_processed = {
    "user": config['db_processed_user'],
    "password": config['db_processed_password'],
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("Pre Process Q4") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

path_to_last_processed_id = 'json/last_processed_id_q4.json'

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
last_id_product = last_ids.get('product_data', 0)
last_id_shop = last_ids.get('shop_data', 0)

# Query para pegar os dados incrementais
query_user_behavior = f"(SELECT * FROM user_behavior_log WHERE id > {last_id_user_behavior}) AS incremental_load_user_behavior"
query_product = f"(SELECT * FROM product_data WHERE id > {last_id_product}) AS incremental_load_product"
query_shop = f"(SELECT * FROM shop_data WHERE id > {last_id_shop}) AS incremental_load_shop"

# Load data from PostgreSQL
user_behavior_data = spark.read.jdbc(url_target, query_user_behavior, properties=db_properties_target)
product_data = spark.read.jdbc(url_target, query_product, properties=db_properties_target)
shop_data = spark.read.jdbc(url_target, query_shop, properties=db_properties_target)

# Update last processed id
update_last_processed_id(path_to_last_processed_id, 'user_behavior_log', user_behavior_data.agg({"id": "max"}).collect()[0][0])
update_last_processed_id(path_to_last_processed_id, 'product_data', product_data.agg({"id": "max"}).collect()[0][0])
update_last_processed_id(path_to_last_processed_id, 'shop_data', shop_data.agg({"id": "max"}).collect()[0][0])

# Join tables
df = user_behavior_data.join(product_data, user_behavior_data['product_id'] == product_data['id'] and user_behavior_data['shop_id'] == product_data['shop_id'], how='inner')
df = df.join(shop_data, df['shop_id'] == shop_data['id'], how='inner')
df = df.select('shop_id', 'shop_name', 'product_name', 'action', 'date', 'product_id')

# Save data to PostgreSQL
df.write.jdbc(url=url_processed, table='pre_process_q4', mode='append', properties=db_properties_processed)
