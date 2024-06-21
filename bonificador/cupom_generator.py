import json
import time
from collections import defaultdict
from sortedcontainers import SortedList
import asyncio
import aio_pika
import os


# Define the conditions for coupons
CONDITIONS = [
    (50_000, 1800 / 3),  # 50,000 monetary units in the last 30 minutes
    (100_000, 3600 * 6)  # 100,000 monetary units in the last hour
]


# Obter a URL do broker de uma variável de ambiente ou usar padrão
RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost/')

# Initialize store purchases
store_purchases = defaultdict(lambda: defaultdict(SortedList))

async def connect_to_rabbitmq():
    return await aio_pika.connect_robust(RABBITMQ_URL)


async def send_coupon_to_queue(user_id, store_id):
    connection = await connect_to_rabbitmq()
    channel = await connection.channel()
    queue = await channel.declare_queue("cupons", durable=True)

    coupon_message = json.dumps({
        "user_id": user_id,
        "store_id": store_id,
        "timestamp": int(time.time())
    })

    await channel.default_exchange.publish(
        aio_pika.Message(body=coupon_message.encode()),
        routing_key="cupons"
    )

    await connection.close()

async def process_order(store_purchases, message: aio_pika.IncomingMessage):
    async with message.process():
        order = json.loads(message.body)
        user_id = order['user_id']
        value = order['price']
        timestamp = order['purchase_date']
        store_id = order['shop_id']

        store_purchases[store_id][user_id].add((timestamp, value))
        
        if await verificar_condicoes(store_id, user_id, store_purchases):
            print(f"Generate coupon for user {user_id} at store {store_id}")
            store_purchases[store_id][user_id].clear()
            await send_coupon_to_queue(user_id, store_id)

async def verificar_condicoes(store_id, user_id, store_purchases):
    current_time = int(time.time())
    for total_value, interval in CONDITIONS:
        total = 0
        purchases = store_purchases[store_id][user_id]
        for timestamp, value in reversed(purchases):
            if current_time - timestamp > interval:
                break
            total += value
        if total >= total_value:
            return True
    return False

async def consume_queue(loja, store_purchases):
    while True:
        try:
            connection = await connect_to_rabbitmq()
            channel = await connection.channel()
            
            # Set prefetch count to limit the number of unacknowledged messages
            await channel.set_qos(prefetch_count=10000)
            
            queue = await channel.declare_queue(loja)
            async for message in queue:
                await process_order(store_purchases, message)
        except aio_pika.exceptions.ConnectionClosed:
            print(f"Connection to {loja} lost. Reconnecting...")
            await asyncio.sleep(5)  # Wait before trying to reconnect
        except Exception as e:
            print(f"Unexpected error: {e}")
            await asyncio.sleep(5)

async def main():
    queues = ['compras_loja1', 'compras_loja2', 'compras_loja3', 'compras_loja4', 'compras_loja5', 'compras_loja6', 'compras_loja7', 'compras_loja8', 'compras_loja9', 'compras_loja10']
    tasks = [consume_queue(loja, store_purchases) for loja in queues]

    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
