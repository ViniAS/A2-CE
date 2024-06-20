# Envio de registros de comportamento para webhook
import random
import datetime
import sys
import os
import my_settings as CONFIG
import message
import mock_utils as _mock

settings = CONFIG.Configuration

class User:
    def __init__(self, id):
        self.id = id
        self.buy_prob = random.random()
        self.MOCK = _mock.MOCK()
        self.MOCK.curr_user_id = 1_000
        self.MOCK.curr_product_id = 1_000

    def send_message(self, user_author_id, action, date,
                     button_product_id, stimulus, component, text_content):
        info = message.Message(user_author_id, action, date,
                               button_product_id, stimulus, component, text_content)
        status_code, response_text = info.send()

        if int(status_code) != 200:
            raise Exception(f"Error sending message: {response_text}")

    def view_item(self):
        data = self.MOCK.generateLogUserBehavior(time_now=True)[:-1]
        user_author_id, action, button_product_id, stimulus, component, text_content, date = data.split(",")
        user_author_id = int(user_author_id)
        date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f").timestamp()
        button_product_id = int(button_product_id)
        print(f"User {user_author_id} viewed item {button_product_id} at {date}")

        self.send_message(user_author_id, action, date,
                          button_product_id, stimulus, component, text_content)