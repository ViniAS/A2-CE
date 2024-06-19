import pika
import json
import time
from collections import deque
# import psycopg2
# from psycopg2 import sql
import threading

# Define the conditions for coupons
# Example: [(total_value, time_interval_in_seconds)]
CONDITIONS = [
    (200_000, 600),  # 100k monetary units in the last 10 minutes
    (400_000, 3600 * 6)  # 250k monetary units in the last 6 hours
]

# Function to connect to RabbitMQ
def connect_to_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    return connection, channel

# Function to connect to PostgreSQL
def connect_to_postgresql():
    connection = psycopg2.connect(
        dbname="your_db_name",
        user="your_db_user",
        password="your_db_password",
        host="your_db_host",
        port="your_db_port"
    )
    return connection

# Function to process the order message
def process_order(channel, method, properties, body):
    order = json.loads(body)
    user_id = order['user_id']
    value = order['price']  # Directly use price since it's already multiplied by quantity
    timestamp = order['purchase_date']
    store_id = order['shop_id']
    #print(f"Received {order}")
    
    # Process the order here (example with deque for sliding window)
    if store_id not in store_purchases:
        store_purchases[store_id] = {}
    if user_id not in store_purchases[store_id]:
        store_purchases[store_id][user_id] = deque()

    # Add order to the deque
    store_purchases[store_id][user_id].append(order)
    
    # Check conditions
    if verificar_condicoes(store_id, user_id):
        compras_relevantes = listar_compras_relevantes(store_id, user_id)
        #clean wsl console
        print(f"Generate coupon for user {user_id} at store {store_id}")
        #sleep for 1 second
        #print("Orders used to generate the coupon:", compras_relevantes)
        # Register the coupon in the database
        #registrar_cupom(user_id, store_id, compras_relevantes)
        # Clear the user's orders
        store_purchases[store_id][user_id].clear()

# Function to check the conditions for coupons
def verificar_condicoes(store_id, user_id):
    current_time = int(time.time())
    for total_value, interval in CONDITIONS:
        total = 0
        for order in store_purchases[store_id][user_id]:
            if current_time - order['purchase_date'] <= interval:
                total += order['price']  # Directly use price since it's already multiplied by quantity
        if total >= total_value:
            return True
    return False

# Function to list relevant orders for coupon generation
def listar_compras_relevantes(store_id, user_id):
    current_time = int(time.time())
    relevant_orders = []
    for total_value, interval in CONDITIONS:
        for order in store_purchases[store_id][user_id]:
            if current_time - order['purchase_date'] <= interval:
                relevant_orders.append(order)
    return relevant_orders

# Function to register the coupon generation in the PostgreSQL database
def registrar_cupom(user_id, store_id, compras_relevantes):
    connection = connect_to_postgresql()
    cursor = connection.cursor()
    
    # Check if the user already has a row in the table
    cursor.execute(sql.SQL("SELECT * FROM cupons WHERE shop_id = %s AND user_id = %s"), (store_id, user_id))
    row = cursor.fetchone()
    
    if row:
        # If the row exists, increment the coupon count
        cursor.execute(sql.SQL("UPDATE cupons SET cupons = cupons + 1 WHERE shop_id = %s AND user_id = %s"), (store_id, user_id))
    else:
        # If the row does not exist, create a new row
        cursor.execute(sql.SQL("INSERT INTO cupons (shop_id, user_id, cupons) VALUES (%s, %s, 1)"), (store_id, user_id))
    
    connection.commit()
    cursor.close()
    connection.close()

# Function to consume messages from a queue
def consume_queue(loja):
    connection, channel = connect_to_rabbitmq()
    channel.queue_declare(queue=loja)
    channel.basic_consume(queue=loja, on_message_callback=process_order, auto_ack=True)
    #print(f' [*] Waiting for messages in {loja}. To exit press CTRL+C')
    channel.start_consuming()

# Main function
def main():
    global store_purchases
    store_purchases = {}

    queues = ['compras_loja1', 'compras_loja2', 'compras_loja3', 'compras_loja4', 'compras_loja5', 'compras_loja6', 'compras_loja7', 'compras_loja8', 'compras_loja9', 'compras_loja10']

    threads = []
    for loja in queues:
        thread = threading.Thread(target=consume_queue, args=(loja,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    main()
