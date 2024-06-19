import random
from faker import Faker # pip install faker
import names # pip install names
from datetime import datetime, timedelta
import datetime
from datetime import datetime

fake = Faker()

stock_dict = dict()

def generate_random_date(min_year=2024, max_year=datetime.now().year):
    start = datetime(min_year, 1, 1, 0, 0, 0)
    years = max_year - min_year
    some_days = datetime.now().month * 30 + datetime.now().day
    end = start + timedelta(days=365 * years + some_days)
    return fake.date_time_between_dates(datetime_start=start, datetime_end=end)

class MOCK:
    def __init__(self):
        self.curr_user_id = 1

        self.curr_product_id = 1

        self.stock_dict = dict()

    def consumer_data(self):
        name = names.get_first_name()
        surname = names.get_last_name()
        city = fake.city()

        user_id = self.curr_user_id
        self.curr_user_id += 1

        born_date = generate_random_date(1950, 2003)
        register_date = generate_random_date(2019, 2023)

        # CREATE TABLE consumer_data (user_id INT, name TEXT, surname TEXT, city TEXT, born_date TIMESTAMP, register_date TIMESTAMP);
        return {"user_id": user_id, "name": name, "surname": surname, 
                "city": city, "born_date": born_date, "register_date": register_date}
    
    def product_data(self):
        product_id = self.curr_product_id
        self.curr_product_id += 1

        name = fake.catch_phrase()
        image = f"{name.replace(' ', '_').lower()}.jpg"
        description = fake.sentence(nb_words=15)
        price = fake.random_int(min=100, max=1000)

        # CREATE TABLE product_data (product_id INT, name TEXT, image TEXT, price INT, description TEXT, shop_id INT);
        return {"product_id": product_id, "name": name, "image": image, "price": price, "description": description, "shop_id": fake.random_int(min=1, max=10)}
    
    def stock_data(self, product_id = -1):
        if product_id == -1:
            product_id = fake.random_int(min=1, max=self.curr_product_id - 1)
        if product_id not in self.stock_dict:
            quantity = fake.random_int(min=1, max=1000)
            self.stock_dict[product_id] = quantity
        else:
            quantity = self.stock_dict[product_id]

        # CREATE TABLE stock_data (product_id INT, quantity INT, shop_id INT);
        return {"product_id": product_id, "quantity": quantity, "shop_id": fake.random_int(min=1, max=10)}
    
    def order_data(self, get_new_date = True):
        user_id = fake.random_int(min=1, max=self.curr_user_id - 1)
        product_id = fake.random_int(min=1, max=self.curr_product_id - 1)
        
        quantity = fake.random_int(min=1, max=10)
        shop = fake.random_int(min=1, max=10)
        price = fake.random_int(min=1, max=100)
        price *= 10
        price *= quantity

        if get_new_date:
            # get 4 random dates
            four_dates = [generate_random_date() for _ in range(4)]

            # sort the dates
            four_dates.sort()

            purchase_date = four_dates[0]
            payment_date = four_dates[1]
            shipping_date = four_dates[2]
            delivery_date = four_dates[3]

        else:
            # Get current date
            purchase_date = datetime.now()
            days_to_pay = fake.random_int(min=1, max=10)
            payment_date = purchase_date + timedelta(days=days_to_pay)
            days_to_ship = fake.random_int(min=1, max=7)
            shipping_date = payment_date + timedelta(days=days_to_ship)
            days_to_deliver = fake.random_int(min=1, max=21)
            delivery_date = shipping_date + timedelta(days=days_to_deliver)

        # CREATE TABLE order_data (user_id INT, product_id INT, quantity INT, purchase_date TIMESTAMP, payment_date TIMESTAMP, shipping_date TIMESTAMP, delivery_date TIMESTAMP, shop_id INT, price INT);
        return {"user_id": user_id, "product_id": product_id, "quantity": quantity, 
                "purchase_date": purchase_date, "payment_date": payment_date, 
                "shipping_date": shipping_date, "delivery_date": delivery_date,
                "shop_id": shop, "price": price}
    
    def shop_data(self, i):
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        shop_id = i + 1
        name = f"Shop {alphabet[i]}"

        # CREATE TABLE shop_data (shop_id INT, shop_name TEXT);
        return {"shop_id": shop_id, "shop_name": name}
    
    def generateLogUserBehavior(self, time_now = False):
        actions = ["click", "hover", "scroll", "drag"]
        components = ["button", "input", "table", "form"]
        stimuli = ["User clicked on a button", "User hovered over an input field",
                   "User scrolled through a table", "User dragged a form element"]

        action = random.choice(actions)
        user_author_id = fake.random_int(min=1, max=self.curr_user_id - 1)
        stimulus = random.choice(stimuli)
        component = random.choice(components)
        text_content = fake.text(max_nb_chars=50)
        date = generate_random_date()
        if time_now:
            date = datetime.now()
        buttonProductId = fake.random_int(min=1, max=self.curr_product_id-1) if action == "click" else 0

        ret = [user_author_id, action, buttonProductId, stimulus, component, text_content, date]
        ret = ','.join(map(str, ret)) + '\n'
        # CREATE TABLE user_behavior_log (user_author_id INT, action TEXT, button_product_id INT, stimulus TEXT, component TEXT, text_content TEXT, date TIMESTAMP);
        return ret
    
    def generateLogAudit(self):
        actions = ["create", "read", "update", "delete"]
        actionOnSystem = ["User created a new account", "User read a document",
                          "User updated a document", "User deleted a document"]
        textContent = fake.text(max_nb_chars=50)

        action = random.choice(actions)
        userAuthorId = fake.random_int(min=1, max=self.curr_user_id - 1)
        actionDescription = random.choice(actionOnSystem)

        date = generate_random_date()

        # CREATE TABLE audit_log (user_author_id INT, action TEXT, action_description TEXT, text_content TEXT, date TIMESTAMP);
        ret = [userAuthorId, action, actionDescription, textContent, date]
        ret = ','.join(map(str, ret)) + '\n'
        return ret
    
    def generateLogFailureNotification(self):
        components = ["database", "server", "client", "network"]
        severities = ["low", "medium", "high", "critical"]
        messages = ["Database connection failed", "Server timeout", "Client error", "Network failure"]
        textContent = fake.text(max_nb_chars=50)

        comp = fake.random_int(min=0, max=len(components) - 1)
        component = components[comp]
        severity = random.choice(severities)
        message = messages[comp]

        date = generate_random_date()

        ret = [component, severity, message, textContent, date]
        ret = ','.join(map(str, ret)) + '\n'
        # CREATE TABLE failure_notification_log (component TEXT, severity TEXT, message TEXT, text_content TEXT, date TIMESTAMP);
        return ret
    
    def generateLogDebug(self):
        messages = ["Debug message 1", "Debug message 2", "Debug message 3", "Debug message 4"]
        textContent = fake.text(max_nb_chars=50)

        message = random.choice(messages)

        date = generate_random_date()

        ret = [message, textContent, date]
        ret = ','.join(map(str, ret)) + '\n'
        # CREATE TABLE debug_log (message TEXT, text_content TEXT, date TIMESTAMP);
        return ret