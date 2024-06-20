# PEga os dados do banco de dados source e faz selects e coloca no banco de dados target

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

url_source = config['db_source_url']
db_properties_source = {
    "user": config['db_source_user'],
    "password": config['db_source_password'],
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("Extractor DB") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

path_to_last_processed_id = 'json/last_processed_id_db.json'

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
last_id_order = last_ids.get('order_data', 0)
last_id_product = last_ids.get('product_data', 0)
last_id_consumer = last_ids.get('consumer_data', 0)
last_id_shop = last_ids.get('shop_data', 0)
last_id_stock = last_ids.get('stock_data', 0)

# Query para pegar os dados incrementais
query_product = f"(SELECT * FROM product_data WHERE id > {last_id_product}) AS incremental_load_product"
query_order = f"(SELECT * FROM order_data WHERE id > {last_id_order}) AS incremental_load_order"
query_consumer = f"(SELECT * FROM consumer_data WHERE id > {last_id_consumer}) AS incremental_load_consumer"
query_shop = f"(SELECT * FROM shop_data WHERE id > {last_id_shop}) AS incremental_load_shop"
query_stock = f"(SELECT * FROM stock_data WHERE id > {last_id_stock}) AS incremental_load_stock"

# Lê os dados do banco de dados
df_order = spark.read.jdbc(url=url_source, table=query_order, properties=db_properties_source)
df_product = spark.read.jdbc(url=url_source, table=query_product, properties=db_properties_source)
df_consumer = spark.read.jdbc(url=url_source, table=query_consumer, properties=db_properties_source)
df_shop = spark.read.jdbc(url=url_source, table=query_shop, properties=db_properties_source)
df_stock = spark.read.jdbc(url=url_source, table=query_stock, properties=db_properties_source)

# Acha o último id processado
max_id_product = df_product.agg({"id": "max"}).collect()[0][0] if not df_product.rdd.isEmpty() else last_id_product
max_id_order = df_order.agg({"id": "max"}).collect()[0][0] if not df_order.rdd.isEmpty() else last_id_order
max_id_consumer = df_consumer.agg({"id": "max"}).collect()[0][0] if not df_consumer.rdd.isEmpty() else last_id_consumer
max_id_shop = df_shop.agg({"id": "max"}).collect()[0][0] if not df_shop.rdd.isEmpty() else last_id_shop
max_id_stock = df_stock.agg({"id": "max"}).collect()[0][0] if not df_stock.rdd.isEmpty() else last_id_stock

# Atualiza o último id processado
update_last_processed_id('last_processed_id_db.json', 'product_data', max_id_product)
update_last_processed_id('last_processed_id_db.json', 'order_data', max_id_order)
update_last_processed_id('last_processed_id_db.json', 'consumer_data', max_id_consumer)
update_last_processed_id('last_processed_id_db.json', 'shop_data', max_id_shop)
update_last_processed_id('last_processed_id_db.json', 'stock_data', max_id_stock)

# Escreve os dados no banco de dados com apenas as colunas necessárias
df_order = df_order.select(['user_id', 'product_id', 'quantity', 'purchase_date', 'shop_id', 'price', 'id'])
df_product = df_product.select(['product_id', 'name', 'price', 'shop_id', 'id'])
df_consumer = df_consumer.select(['user_id', 'id'])

# Escreve os dados no banco de dados, se a tabela não existir, cria
df_order.write.jdbc(url=url_target, table="order_data", mode="append", properties=db_properties_target)
df_product.write.jdbc(url=url_target, table="product_data", mode="append", properties=db_properties_target)
df_consumer.write.jdbc(url=url_target, table="consumer_data", mode="append", properties=db_properties_target)
df_shop.write.jdbc(url=url_target, table="shop_data", mode="append", properties=db_properties_target)
df_stock.write.jdbc(url=url_target, table="stock_data", mode="append", properties=db_properties_target)