# Envio de registros de comportamento para webhook
import random
import datetime
import sys
import os
import my_settings as CONFIG
import message

settings = CONFIG.Configuration

class User:
    def __init__(self, id):
        self.id = id
        self.buy_prob = random.random()

    def send_message(self, shop_id, product_id, behavior, timestamp):
        info = message.Message(shop_id, self.id, product_id,
                               behavior, timestamp)
        status_code, response_text = info.send()

        if int(status_code) != 200:
            raise Exception(f"Error sending message: {response_text}")

    def view_item(self):
        shop_id = random.randint(1, settings.NUM_SHOPS + 1)
        product_id = random.randint(1, settings.NUM_PRODUCTS + 1)
        timestamp = datetime.datetime.now()
        self.send_message(shop_id, product_id, "view", timestamp)

        if random.random() < self.buy_prob:
            self.send_message(shop_id, product_id, "buy", timestamp)