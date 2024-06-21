# Pega colunas do target_db e faz operações de join e deixar a tabela pronta para ser acessada pelo monitor de preços
import time
from pyspark.sql import SparkSession
import json

TEMPO_ESPERA = 10
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
    .appName("Pre Process Monitor") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

path_to_last_processed_id = 'json/last_processed_id_monitor.json'

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

while True:

    # Acha o último id processado
    last_ids = get_last_processed_ids(path_to_last_processed_id)
    last_id_order = last_ids.get('order_data', 0)
    last_id_product = last_ids.get('product_data', 0)
    last_id_shop = last_ids.get('shop_data', 0)

    # Query para pegar os dados incrementais
    query_order = f"(SELECT * FROM order_data WHERE id > {last_id_order}) AS incremental_load_order"
    query_product = f"(SELECT * FROM product_data WHERE id > {last_id_product}) AS incremental_load_product"
    query_shop = f"(SELECT * FROM shop_data WHERE id > {last_id_shop}) AS incremental_load_shop"

    # Load data from PostgreSQL
    order_data = spark.read.jdbc(url_target, query_order, properties=db_properties_target)
    product_data = spark.read.jdbc(url_target, query_product, properties=db_properties_target)
    shop_data = spark.read.jdbc(url_target, query_shop, properties=db_properties_target)


    # Join tables
    # product_data.show()
    # shop_data.show()
    # order_data.show()
    product_data = product_data.select(['product_id', 'name', 'id'])
    order_data = order_data.select(['product_id', 'price', 'purchase_date', 'shop_id', 'id'])
    df_0 = order_data.join(shop_data, ['shop_id'], how='inner')
    df = df_0.join(product_data, ['product_id'], how='inner')
    
    df = df.select(['name', 'price', 'purchase_date', 'shop_name', product_data['product_id']])
    
    if df.count() == 0:
        print("No new data to process. Waiting for new data...")
        time.sleep(TEMPO_ESPERA)
        continue
    # Save data to PostgreSQL
    df.write.jdbc(url=url_processed, table='pre_process_monitor', mode='append', properties=db_properties_processed)

    # Update last processed id
    update_last_processed_id(path_to_last_processed_id, 'order_data', order_data.agg({"id": "max"}).collect()[0][0])
    update_last_processed_id(path_to_last_processed_id, 'product_data', product_data.agg({"id": "max"}).collect()[0][0])
    update_last_processed_id(path_to_last_processed_id, 'shop_data', shop_data.agg({"id": "max"}).collect()[0][0])
    time.sleep(TEMPO_ESPERA)


