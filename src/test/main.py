import psycopg2
from celery import Celery

# Create a Celery instance and configure it to use the RabbitMQ message broker
app = Celery('tasks', broker='pyamqp://guest@rabbit//')

@app.task
def insert_into_db(data):
    conn = psycopg2.connect("dbname=test user=postgres password=secret host=db")
    cur = conn.cursor()
    # Assuming data is a dictionary containing the values to be inserted
    cur.execute("CREATE TABLE IF NOT EXISTS your_table (id serial PRIMARY KEY, column1 varchar, column2 varchar);")
    cur.execute("INSERT INTO your_table (column1, column2) VALUES (%s, %s)", (data['value1'], data['value2']))
    conn.commit()
    cur.close()
    conn.close()

# Call the task
insert_into_db.delay({'value1': 'some value', 'value2': 'some other value'})