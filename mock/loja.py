# Registros live (BD)
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import os
import time
import mock_utils as _mock
import random
import pika

# Load configuration from config.json
with open("src/config.json") as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"

# Create a Spark session
spark = SparkSession.builder \
     .appName("PySpark PostgreSQL Live Data") \
     .config("spark.jars", jdbc_driver_path) \
     .getOrCreate()

# Database connection properties
url = config['db_source_url']
properties = {
    "user": config['db_source_user'],
    "password": config['db_source_password'],
    "driver": "org.postgresql.Driver"
}

def connect_to_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    return connection, channel

def declare_queues(channel, lojas):
    for loja in lojas:
        channel.queue_declare(queue=loja)

def generate_purchase(user_id, product_id, quantity, purchase_date, payment_date, shipping_date, delivery_date, shop_id, price):
    return {
        'user_id': user_id,
        'product_id': product_id,
        'quantity': quantity,
        'purchase_date': purchase_date,
        'payment_date': payment_date,
        'shipping_date': shipping_date,
        'delivery_date': delivery_date,
        'shop_id': shop_id,
        'price': price
    }

def send_purchase(channel, loja, purchase):
    channel.basic_publish(
        exchange='',
        routing_key=loja,
        body=json.dumps(purchase)
    )
    print(f"Enviado: {purchase}")

lojas = ['compras_loja'+str(i) for i in range(1, 11)]
connection, channel = connect_to_rabbitmq()
declare_queues(channel, lojas)

MOCK = _mock.MOCK()
MOCK.curr_user_id = 1_000
MOCK.curr_product_id = 1_000

count_time = 0
while True:
    # Every 5 seconds, generate 10 new order data and write it to the database
    time.sleep(1)
    count_time += 1
    # Random int between 1 and 10
    qtd = random.randint(50, 150)
    order_data = [MOCK.order_data(get_new_date=False) for _ in range(qtd)]
    try:
        for order in order_data:
            user_id = order['user_id']
            product_id = order['product_id']
            quantity = order['quantity']
            purchase_date = order['purchase_date']
            # Convert datetime to timestamp
            purchase_date = purchase_date.timestamp()
            payment_date = order['payment_date']
            payment_date = payment_date.timestamp()
            shipping_date = order['shipping_date']
            shipping_date = shipping_date.timestamp()
            delivery_date = order['delivery_date']
            delivery_date = delivery_date.timestamp()
            shop_id = order['shop_id']
            price = order['price']

            purchase = generate_purchase(user_id, product_id, quantity, purchase_date, payment_date, shipping_date, delivery_date, shop_id, price)
            send_purchase(channel, shop_id, purchase)
    except Exception as e:
        print(f"Error: {e}")
        connection.close()
    try:
        df = spark.createDataFrame(order_data)
        df.write.jdbc(url=url, table="order_data", mode="append", properties=properties)
    except Exception as e:
        print(f"Error: {e}")
        spark.stop()
        break
    if count_time % 5 == 0:
        qtd = random.randint(1, 15)
        user_data = [MOCK.consumer_data() for _ in range(qtd)]
        try:
            df = spark.createDataFrame(user_data)
            df.write.jdbc(url=url, table="consumer_data", mode="append", properties=properties)
        except Exception as e:
            print(f"Error: {e}")
            spark.stop()
            break
    if count_time % 10 == 0:
        count_time = 0
        qtd = random.randint(1, 5)
        product_data = [MOCK.product_data() for _ in range(qtd)]
        try:
            df = spark.createDataFrame(product_data)
            df.write.jdbc(url=url, table="product_data", mode="append", properties=properties)
        except Exception as e:
            print(f"Error: {e}")
            spark.stop()
            break
        stock_data = [MOCK.stock_data() for _ in range(qtd)]
        try:
            df = spark.createDataFrame(stock_data)
            df.write.jdbc(url=url, table="stock_data", mode="append", properties=properties)
        except Exception as e:
            print(f"Error: {e}")
            spark.stop()
            break