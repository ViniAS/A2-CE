# Pega colunas do target_db e faz operações de join e deixar a tabela pronta para ser acessada pela pergunta 2

from pyspark.sql import SparkSession
import json
import time


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
    .appName("Pre Process Q2") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

path_to_last_processed_id = 'json/last_processed_id_q2.json'

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

while(True):

    # Acha o último id processado
    last_ids = get_last_processed_ids(path_to_last_processed_id)
    last_id_order = last_ids.get('order_data', 0)
    last_id_shop = last_ids.get('shop_data', 0)

    # Query para pegar os dados incrementais
    query_order = f"(SELECT * FROM order_data WHERE id > {last_id_order}) AS incremental_load_order"
    query_shop = f"(SELECT * FROM shop_data WHERE id > {last_id_shop}) AS incremental_load_shop"

    # Load data from PostgreSQL
    order_data = spark.read.jdbc(url_target, query_order, properties=db_properties_target)
    shop_data = spark.read.jdbc(url_target, query_shop, properties=db_properties_target)


    # Join tables
    shop_data = shop_data.withColumnRenamed('shop_id', 'shopid')
    df = order_data.join(shop_data, order_data.shop_id == shop_data.shopid, how='inner')
    df = df.select(['shop_id', 'price', 'quantity', 'purchase_date'])
    if df.count() == 0:
        print("No new data found. Waiting for 10 seconds.")
        time.sleep(TEMPO_ESPERA)
        continue
        

    # Save data to PostgreSQL
    df.write.jdbc(url=url_processed, table='pre_process_q2', mode='append', properties=db_properties_processed)

    # Update last processed id
    update_last_processed_id(path_to_last_processed_id, 'order_data', order_data.agg({"id": "max"}).collect()[0][0])
    update_last_processed_id(path_to_last_processed_id, 'shop_data', shop_data.agg({"id": "max"}).collect()[0][0])

    time.sleep(TEMPO_ESPERA)