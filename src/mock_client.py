from __future__ import print_function

import logging
import random
from time import sleep

import mock
from multiprocessing import Process
from mock_server import store_user_behavior
import json
from pyspark.sql import SparkSession


def log_user_behavior_messages(num_messages: int = 1000):

    for _ in range(num_messages):
        message = (mock.generateLogUserBehavior())
        # send message to server
        store_user_behavior.apply_async((message,), priority=0)
        sleep(1)

if __name__ == '__main__':
    processes = []
    for i in range(12):
        process = Process(target=log_user_behavior_messages, args=())
        process.start()
        processes.append(process)

    with open('config.json') as f:
        config = json.load(f)

    # Path do driver JDBC do PostgreSQL
    jdbc_driver_path = "../jdbc/postgresql-42.7.3.jar"

    # Propriedades de conexão com o banco de dados
    db_properties = {
        "user": config['db_user'],
        "password": config['db_password'],
        "driver": "org.postgresql.Driver",
        "url": config['db_url']
    }
    spark = SparkSession.builder \
        .appName("Store Log") \
        .config("spark.jars", jdbc_driver_path) \
        .config("spark.ui.port", "60050") \
        .getOrCreate()

    while True:
        db_df = spark.read.jdbc(url=db_properties['url'], table="log_user_behavior", properties=db_properties)
        db_df.show()
        print("Number of rows: ", db_df.count())
        sleep(5)

