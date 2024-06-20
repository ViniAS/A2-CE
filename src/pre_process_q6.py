# Pega colunas do target_db e faz operações de join e deixar a tabela pronta para ser acessada pela pergunta 6

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
    .appName("Pre Process Q6") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

path_to_last_processed_id = 'json/last_processed_id_q6.json'

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
last_id_stock = last_ids.get('stock_data', 0)

# Query para pegar os dados incrementais
query_order = f"(SELECT * FROM order_data WHERE id > {last_id_order}) AS incremental_load_order"
query_stock = f"(SELECT * FROM stock_data WHERE id > {last_id_stock}) AS incremental_load_stock"

# Load data from PostgreSQL
order_data = spark.read.jdbc(url_target, query_order, properties=db_properties_target)
stock_data = spark.read.jdbc(url_target, query_stock, properties=db_properties_target)

# Join tables
stock_data = stock_data.withColumnRenamed('quantity', 'stock_quantity')
df = order_data.join(stock_data, order_data['product_id'] == stock_data['stock_quantity'])
df = df.select(stock_data.stock_quantity, order_data.quantity, order_data.shop_id)

# Save data to PostgreSQL
df.write.jdbc(url=url_processed, table='pre_process_q6', mode='append', properties=db_properties_processed)

# Update last processed id
update_last_processed_id(path_to_last_processed_id, 'order_data', order_data.agg({"id": "max"}).collect()[0][0])
update_last_processed_id(path_to_last_processed_id, 'stock_data', stock_data.agg({"id": "max"}).collect()[0][0])
