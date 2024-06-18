import requests
import sys
import os
import my_settings as CONFIG


class Message:
    def __init__(self, shop_id, user_id, product_id, behavior, datetime):
        self.shop_id = shop_id
        self.user_id = user_id
        self.product_id = product_id
        self.behavior = behavior
        self.datetime = datetime

        self.json = {
            "shop_id": self.shop_id,
            "user_id": self.user_id,
            "product_id": self.product_id,
            "behavior": self.behavior,
            "datetime": self.datetime
        }

        self.url = CONFIG.Configuration.URL

    def send(self):
        response = requests.post(self.url, json=self.json)

        return response.status_code, response.text

    def __repr__(self):
        return f"Message(shop_id={self.shop_id}, user_id={self.user_id}, product_id={self.product_id}, behavior={self.behavior}, datetime={self.datetime})"

    def __str__(self):
        return f"User {self.user_id} {self.behavior} item {self.product_id} at {self.datetime} in shop {self.shop_id}"