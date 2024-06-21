import pika
import json
import time
import random

def connect_to_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    return connection, channel

def declare_queues(channel, lojas):
    for loja in lojas:
        channel.queue_declare(queue=loja)

def generate_purchase(user_id, value, timestamp, product_id, store_id):
    return {
        'user_id': user_id,
        'value': value,
        'timestamp': timestamp,
        'product_id': product_id,
        'store_id': store_id
    }

def send_purchase(channel, loja, purchase):
    channel.basic_publish(
        exchange='',
        routing_key=loja,
        body=json.dumps(purchase)
    )
    print(f"Enviado: {purchase}")

def main():
    lojas = ['compras_loja1', 'compras_loja2','compras_loja3', 'compras_loja4']  # Adicione todas as lojas
    connection, channel = connect_to_rabbitmq()
    declare_queues(channel, lojas)

    user_ids = [i for i in range(1,10000)]
    values = [100, 200, 300, 400, 500]
    product_ids = [101, 102, 103, 104, 105]

    try:
        n=20000
        for _ in range(n):  # Envia n mensagens para cada fila
            user_id = random.choice(user_ids)
            value = random.choice(values)
            product_id = random.choice(product_ids)
            timestamp = int(time.time())
            store_id = random.choice(lojas)
            purchase = generate_purchase(user_id, value, timestamp, product_id, store_id)
            send_purchase(channel, store_id, purchase)
            time.sleep(0.01)
    except KeyboardInterrupt:
        connection.close()

if __name__ == '__main__':
    main()
