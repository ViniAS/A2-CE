import requests
import sys
import os
import my_settings as CONFIG


class Message:
    def __init__(self, user_author_id, button_product_id, stimulus, date, action, component, text_content):
        self.user_author_id = user_author_id
        self.button_product_id = button_product_id
        self.stimulus = stimulus
        self.date = date
        self.action = action
        self.component = component
        self.text_content = text_content
        
        self.json = {
            "user_author_id": self.user_author_id,
            "button_product_id": self.button_product_id,
            "stimulus": self.stimulus,
            "date": self.date,
            "action": self.action,
            "component": self.component,
            "text_content": self.text_content
        }

        self.url = CONFIG.Configuration.URL

    def send(self):
        response = requests.post(self.url, json=self.json)

        return response.status_code, response.text

    def __repr__(self):
        return f"Message({self.user_author_id}, {self.button_product_id}, {self.stimulus}, {self.date}, {self.action}, {self.component}, {self.text_content})"

    def __str__(self):
        return f"User: {self.user_author_id}, Product: {self.button_product_id}, Stimulus: {self.stimulus}, Date: {self.date}, Action: {self.action}, Component: {self.component}, Text: {self.text_content}"