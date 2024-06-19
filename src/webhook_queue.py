from celery import Celery
import psycopg2
from psycopg2 import pool
import json
from pyspark.sql import SparkSession
import datetime
# Run with "celery -A webhook_queue worker --loglevel=INFO --concurrency=10"


app = Celery('tasks', broker='pyamqp://guest@localhost//')

with open('config.json') as f:
    config = json.load(f)

# Path do driver JDBC do PostgreSQL
jdbc_driver_path = "../jdbc/postgresql-42.7.3.jar"

# Propriedades de conexão com o banco de dados
db_properties = {
    "user": config['db_target_user'],
    "password": config['db_target_password'],
    "host": "localhost",
    "port": "5432",
    "dbname": "target_db"
}

db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **db_properties)


@app.task
def store_user_behavior(message: str):
    """ Takes the message from the webhook and stores it in the database

    Args:
        message (str): The message from the webhook, in JSON format
    """
    
    message = json.loads(message)

    # Convert date from timestamp to datetime
    message['date'] = datetime.datetime.fromtimestamp(message['date'])

    # Get a connection from the pool
    conn = db_pool.getconn()

    try:
        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Execute a query
        cur.execute(
            """INSERT INTO user_behavior_log (
                user_author_id, 
                action, 
                date, 
                button_product_id, 
                stimulus,
                component,
                text_content
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (
                message['user_author_id'],
                message['action'],
                message['date'],
                message['button_product_id'],
                message['stimulus'],
                message['component'],
                message['text_content']
            )
        )

        # Commit the transaction
        conn.commit()

    finally:
        # Close the cursor and the connection (returns it to the pool)
        if cur is not None:
            cur.close()
        db_pool.putconn(conn)



