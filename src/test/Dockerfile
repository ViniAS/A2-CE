FROM python:3.8-slim-buster

WORKDIR /app

COPY . /app

RUN pip install psycopg2-binary \
    && pip install celery

CMD ["python", "./main.py"]