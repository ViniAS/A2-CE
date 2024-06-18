from celery import Celery
import psycopg2
from psycopg2 import pool
import json
from pyspark.sql import SparkSession
# Run with "celery -A mock_server worker --loglevel=INFO --concurrency=10"


app = Celery('tasks', broker='pyamqp://guest@localhost//')

with open('config.json') as f:
    config = json.load(f)

# Path do driver JDBC do PostgreSQL
jdbc_driver_path = "../jdbc/postgresql-42.7.3.jar"

# Propriedades de conexão com o banco de dados
db_properties = {
    "user": config['db_user'],
    "password": config['db_password'],
    "host": "localhost",
    "port": "5432",
    "dbname": "source_db"
}

db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **db_properties)


@app.task
def store_user_behavior(message: str):
    """ Takes the message from the webhook and stores it in the database

    Args:
        message (str): The message from the webhook, in JSON format
    """
    
    message = json.loads(message)

    # Get a connection from the pool
    conn = db_pool.getconn()

    try:
        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Execute a query
        cur.execute(
            "INSERT INTO webhook (shop_id, user_id, product_id, behavior, datetime) VALUES (%s, %s, %s, %s, %s)",
            (message['shop_id'], message['user_id'], message['product_id'], message['behavior'],
             message['datetime'])
        )

        # Commit the transaction
        conn.commit()

    finally:
        # Close the cursor and the connection (returns it to the pool)
        if cur is not None:
            cur.close()
        db_pool.putconn(conn)



