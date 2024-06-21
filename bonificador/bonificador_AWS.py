# import json
# import time
# from collections import defaultdict
# from sortedcontainers import SortedList
# import asyncio
# import boto3
# from botocore.exceptions import ClientError

# # Define the conditions for coupons
# CONDITIONS = [
#     (5_000, 1800 / 3),  # 50,000 monetary units in the last 30 minutes
#     (10_000, 3600 * 6)  # 100,000 monetary units in the last hour
# ]

# # Initialize store purchases
# store_purchases = defaultdict(lambda: defaultdict(SortedList))

# # Initialize boto3 client for SQS
# sqs_client = boto3.client('sqs', region_name='us-east-1')  # Replace 'your-region' with the appropriate AWS region

# # URL of the SQS queue for sending coupons
# COUPON_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/832766163897/cupons'  # Replace with the URL of your coupon SQS queue

# async def send_coupon_to_queue(user_id, store_id):
#     coupon_message = json.dumps({
#         "user_id": user_id,
#         "store_id": store_id,
#         "timestamp": int(time.time())
#     })

#     try:
#         response = sqs_client.send_message(
#             QueueUrl=COUPON_QUEUE_URL,
#             MessageBody=coupon_message
#         )
#         print(f"Coupon sent: {response['MessageId']}")
#     except ClientError as e:
#         print(f"Error sending coupon: {e}")

# async def process_order(store_purchases, message):
#     order = json.loads(message['Body'])
#     user_id = order['user_id']
#     value = order['price']
#     timestamp = order['purchase_date']
#     store_id = order['shop_id']

#     store_purchases[store_id][user_id].add((timestamp, value))
    
#     if await verificar_condicoes(store_id, user_id, store_purchases):
#         print(f"Generate coupon for user {user_id} at store {store_id}")
#         store_purchases[store_id][user_id].clear()
#         await send_coupon_to_queue(user_id, store_id)
#     print("processed{}".format(order))

# async def verificar_condicoes(store_id, user_id, store_purchases):
#     current_time = int(time.time())
#     for total_value, interval in CONDITIONS:
#         total = 0
#         purchases = store_purchases[store_id][user_id]
#         for timestamp, value in reversed(purchases):
#             if current_time - timestamp > interval:
#                 break
#             total += value
#         if total >= total_value:
#             return True
#     return False

# async def consume_queue(queue_url, store_purchases):
#     while True:
#         try:
#             response = sqs_client.receive_message(
#                 QueueUrl=queue_url,
#                 MaxNumberOfMessages=10,
#                 WaitTimeSeconds=20
#             )
            
#             if 'Messages' in response:
#                 for message in response['Messages']:
#                     await process_order(store_purchases, message)
                    
#                     # Delete message from the queue after processing
#                     sqs_client.delete_message(
#                         QueueUrl=queue_url,
#                         ReceiptHandle=message['ReceiptHandle']
#                     )
#             else:
#                 await asyncio.sleep(5)
#         except ClientError as e:
#             print(f"Unexpected error: {e}")
#             await asyncio.sleep(5)
#         print("consumed")
        

# async def main():
#     queues = [f'https://sqs.us-east-1.amazonaws.com/832766163897/compras_loja{i}' for i in range(1, 11)]
#     print(queues)
#     print("Starting consumers...")
#     tasks = [consume_queue(queue_url, store_purchases) for queue_url in queues]

#     await asyncio.gather(*tasks)

# if __name__ == '__main__':
#     asyncio.run(main())

import json
import time
from collections import defaultdict
from sortedcontainers import SortedList
import asyncio
import boto3
from botocore.exceptions import ClientError

# Define the conditions for coupons
CONDITIONS = [
    (5_000, 1800 / 3),  # 50,000 monetary units in the last 30 minutes
    (10_000, 3600 * 6)  # 100,000 monetary units in the last hour
]

# Initialize store purchases
store_purchases = defaultdict(lambda: defaultdict(SortedList))

# Initialize boto3 client for SQS
sqs_client = boto3.client('sqs', region_name='us-east-1')  # Replace 'your-region' with the appropriate AWS region

# URL of the SQS queue for sending coupons
COUPON_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/832766163897/cupons'  # Replace with the URL of your coupon SQS queue

async def send_coupon_to_queue(user_id, store_id):
    coupon_message = json.dumps({
        "user_id": user_id,
        "store_id": store_id,
        "timestamp": int(time.time())
    })

    try:
        response = sqs_client.send_message(
            QueueUrl=COUPON_QUEUE_URL,
            MessageBody=coupon_message
        )
        print(f"Coupon sent: {response['MessageId']}")
    except ClientError as e:
        print(f"Error sending coupon: {e}")

async def process_order(store_purchases, message):
    order = json.loads(message['Body'])
    user_id = order['user_id']
    value = order['price']
    timestamp = order['purchase_date']
    store_id = order['shop_id']

    store_purchases[store_id][user_id].add((timestamp, value))
    
    if await verificar_condicoes(store_id, user_id, store_purchases):
        print(f"Generate coupon for user {user_id} at store {store_id}")
        store_purchases[store_id][user_id].clear()
        await send_coupon_to_queue(user_id, store_id)
    print(f"Processed order: {order}")

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

async def consume_queue(queue_url, store_purchases):
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )
            
            if 'Messages' in response:
                tasks = []
                for message in response['Messages']:
                    tasks.append(process_order(store_purchases, message))
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                await asyncio.gather(*tasks)
            else:
                await asyncio.sleep(5)
        except ClientError as e:
            print(f"Unexpected error: {e}")
            await asyncio.sleep(5)
        print(f"Consumed from queue: {queue_url}")

async def main():
    queues = [f'https://sqs.us-east-1.amazonaws.com/832766163897/compras_loja{i}' for i in range(1, 11)]
    print(queues)
    print("Starting consumers...")
    tasks = [consume_queue(queue_url, store_purchases) for queue_url in queues]

    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
