import random
from faker import Faker # pip install faker
import names # pip install names
from datetime import datetime, timedelta
import datetime
from datetime import datetime, timedelta

fake = Faker()

stock_dict = dict()

# controls the range of random numbers generated
your_computer_id = 0

# Utils
def generate_random_date(min_year=2020, max_year=datetime.now().year):
    start = datetime(min_year, 1, 1, 0, 0, 0)
    years = max_year - min_year + 1
    some_days = datetime.now().month * 30 + datetime.now().day
    end = start + timedelta(days=365 * years + some_days)
    return fake.date_time_between_dates(datetime_start=start, datetime_end=end)

# CSV data generators
# All for Conta Verde:

def consumer_data():
    name = names.get_first_name()
    surname = names.get_last_name()
    city = fake.city()

    user_id = fake.random_int(min = 1+ your_computer_id*1000, max = 1000+your_computer_id*1000)

    born_date = generate_random_date(1950, 1960 + your_computer_id*10)
    register_date = generate_random_date(2019, 2021)
    return {"user_id": user_id, "name": name, "surname": surname, 
            "city": city, "born_date": born_date, "register_date": register_date}

def product_data():
    product_id = fake.random_int(min = 1+your_computer_id*10, max = 10+your_computer_id*10)
    name = fake.catch_phrase()
    image = f"{name.replace(' ', '_').lower()}.jpg"
    description = fake.sentence(nb_words=15)
    price = fake.random_int(min=100, max=1000)

    return {"product_id": product_id, "name": name, "image": image, "price": price, "description": description}

def stock_data(product_id = -1):
    if product_id not in stock_dict:
        quantity = fake.random_int(min=1, max=1000)
        stock_dict[product_id] = quantity
    else:
        quantity = stock_dict[product_id]
    return {"product_id": product_id, "quantity": quantity}

def order_data(get_new_date = True):
    user_id = fake.random_int(min=1+your_computer_id*1000, max=1000+your_computer_id*1000)
    product_id = fake.random_int(min=1+ your_computer_id*10, max=10+your_computer_id*10)
    quantity = fake.random_int(min=1, max=10)
    shop = fake.random_int(min=1, max=50)
    price = fake.random_int(min=1, max=100)
    price *= 10
    price *= quantity

    if get_new_date:
        # get 4 random dates
        four_dates = [generate_random_date(2019, datetime.now().year) for _ in range(4)]

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



    return {"user_id": user_id, "product_id": product_id, "quantity": quantity, 
            "purchase_date": purchase_date, "payment_date": payment_date, 
            "shipping_date": shipping_date, "delivery_date": delivery_date,
            "shop_id": shop, "price": price}

# Log data generators
# DataCat & CadêAnalytics:
def generateLogUserBehavior():
    actions = ["click", "hover", "scroll", "drag"]
    components = ["button", "input", "table", "form"]
    stimuli = ["User clicked on a button", "User hovered over an input field",
               "User scrolled through a table", "User dragged a form element"]

    action = random.choice(actions)
    user_author_id = fake.random_int(min=1+your_computer_id*1000, max=1000+your_computer_id*1000)
    stimulus = random.choice(stimuli)
    component = random.choice(components)
    text_content = fake.text(max_nb_chars=50)
    date = generate_random_date(2022, 2023)
    buttonProductId = fake.random_int(min=1, max=7) if action == "click" else 0

    return {"user_author_id": user_author_id, "action": action, "button_product_id": buttonProductId,
            "stimulus": stimulus, "component": component, "text_content": text_content, "date": date}

# DataCat Only:

def generateLogAudit():
    actions = ["create", "read", "update", "delete"]
    actionOnSystem = ["User created a new account", "User read a document",
                      "User updated a document", "User deleted a document"]
    textContent = fake.text(max_nb_chars=50)

    action = random.choice(actions)
    userAuthorId = fake.random_int(min=1, max=1000)
    actionDescription = random.choice(actionOnSystem)

    date = generate_random_date(2019, 2023)

    return {"user_author_id": userAuthorId, "action": action, "action_description": actionDescription,
            "text_content": textContent, "date": date}

def generateLogFailureNotification():
    components = ["database", "server", "client", "network"]
    severities = ["low", "medium", "high", "critical"]
    messages = ["Database connection failed", "Server timeout", "Client error", "Network failure"]
    textContent = fake.text(max_nb_chars=50)

    comp = fake.random_int(min=0, max=len(components) - 1)
    component = components[comp]
    severity = random.choice(severities)
    message = messages[comp]

    date = generate_random_date(2019, 2023)

    return {"component": component, "severity": severity, "message": message, "text_content": textContent, "date": date}

def generateLogDebug():
    messages = ["Debug message 1", "Debug message 2", "Debug message 3", "Debug message 4"]
    textContent = fake.text(max_nb_chars=50)

    message = random.choice(messages)

    date = generate_random_date(2019, 2023)

    return {"message": message, "text_content": textContent, "date": date}