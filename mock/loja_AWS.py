import threading
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import os
import time
import mock_utils as _mock
import random
import boto3
from botocore.exceptions import ClientError

# Load configuration from config.json
with open("src/config_AWS.json") as f:
    config = json.load(f)

# Path to the PostgreSQL JDBC driver
# Path to the PostgreSQL JDBC driver
jdbc_driver_path = "/home/ec2-user/jdbc/postgresql-42.7.3.jar"

# Create a Spark session
spark = SparkSession.builder \
     .appName("PySpark PostgreSQL Live Data") \
     .config("spark.jars", jdbc_driver_path) \
     .getOrCreate()


# Database connection properties for Amazon RDS
url = config['rds_url']
properties = {
    "user": config['rds_user'],
    "password": config['rds_password'],
    "driver": "org.postgresql.Driver",
    "dbname": "historico-instance-1"
}

# Initialize boto3 client for SQS
sqs_client = boto3.client('sqs', region_name='us-east-1')  # Replace 'your-region' with the appropriate AWS region
rds_client = boto3.client('rds', region_name='us-east-1')  # Replace 'your-region' with the appropriate AWS region

# URLs of the SQS queues
LOJA_QUEUES = [f'https://sqs.us-east-1.amazonaws.com/832766163897/compras_loja{i}' for i in range(1, 11)]  # Replace with your SQS queue URLs
COUPON_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/832766163897/cupons'  # Replace with the URL of your coupon SQS queue

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

def send_purchase(queue_url, purchase):
    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(purchase)
        )
        print(f"Enviado: {purchase}")
    except ClientError as e:
        print(f"Failed to send message: {e}")

def consume_coupons_queue():
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=COUPON_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )
            
            if 'Messages' in response:
                for message in response['Messages']:
                    # Process the coupon message
                    #print(f"Coupon consumed and discarded: {message['Body']}")
                    
                    # Delete the message from the queue after processing
                    sqs_client.delete_message(
                        QueueUrl=COUPON_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
        except ClientError as e:
            print(f"Unexpected error: {e}")
            time.sleep(5)

# Start the coupons queue consumer in a separate thread
consumer_thread = threading.Thread(target=consume_coupons_queue)
consumer_thread.start()

MOCK = _mock.MOCK()
MOCK.curr_user_id = 1_000
MOCK.curr_product_id = 1_000

count_time = 0
while True:
    # Every 5 seconds, generate 10 new order data and write it to the database
    time.sleep(1)
    count_time += 1
    # Random int between 50 and 150
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
            send_purchase(LOJA_QUEUES[shop_id - 1], purchase)  # Send to appropriate queue based on shop_id
    except Exception as e:
        print(f"Error: {e}")
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

