#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE target_db;
    GRANT ALL PRIVILEGES ON DATABASE target_db TO $POSTGRES_USER;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE processed_db;
    GRANT ALL PRIVILEGES ON DATABASE processed_db TO $POSTGRES_USER;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "processed_db" <<-EOSQL
    CREATE TABLE pre_process_q1 (purchase_date TIMESTAMP,
    quantity INT,
    product_id INT,
    shop_id INT
);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "processed_db" <<-EOSQL
    CREATE TABLE pre_process_q2 (shop_id INT,
    price INT,
    quantity INT,
    purchase_date TIMESTAMP
);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "processed_db" <<-EOSQL
    CREATE TABLE pre_process_q3 (button_product_id INT,
    date TIMESTAMP,
    user_author_id INT,
    shop_id INT,
    action TEXT
);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "processed_db" <<-EOSQL
    CREATE TABLE pre_process_q4 (shop_id INT,
    shop_name TEXT,
    name TEXT,
    action TEXT,
    date TIMESTAMP,
    product_id INT
);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "processed_db" <<-EOSQL
    CREATE TABLE pre_process_q5 (user_author_id INT,
    shop_id INT,
    action TEXT,
    date TIMESTAMP,
    purchase_date TIMESTAMP,
    product_id INT
);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "processed_db" <<-EOSQL
    CREATE TABLE pre_process_q6 (stock_quantity INT,
    quantity INT,
    shop_id INT
);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "processed_db" <<-EOSQL
    CREATE TABLE pre_process_monitor (name TEXT,
    price INT,
    purchase_date TIMESTAMP,
    shop_name TEXT,
    product_id INT
);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE user_behavior_log (id SERIAL PRIMARY KEY,
    user_author_id INT,
    action VARCHAR(100),
    button_product_id INT,
    date TIMESTAMP
);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE consumer_data (id INT,
    user_id INT);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE product_data (id INT,
    product_id INT,
    name TEXT,
    price INT,
    shop_id INT);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE stock_data (id INT,
    product_id INT,
    quantity INT,
    shop_id INT);
EOSQL



psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE order_data (id INT,
    user_id INT,
    product_id INT,
    quantity INT,
    purchase_date TIMESTAMP,
    shop_id INT,
     price INT);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE shop_data (id INT,
    shop_id INT,
     shop_name TEXT);
EOSQL



psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE log_audit (id SERIAL PRIMARY KEY,
    user_author_id INT,
    action TEXT,
    action_description TEXT,
    text_content TEXT,
    date TIMESTAMP);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE log_failure (id SERIAL PRIMARY KEY,
    component TEXT,
    severity TEXT,
    message TEXT, 
    text_content TEXT,
     date TIMESTAMP);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE log_debug (id SERIAL PRIMARY KEY,
    message TEXT, 
    text_content TEXT, 
    date TIMESTAMP);
EOSQL



psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE consumer_data (id SERIAL PRIMARY KEY,
    user_id INT,
    name TEXT,
    surname TEXT,
    city TEXT,
    born_date TIMESTAMP,
    register_date TIMESTAMP);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE product_data (id SERIAL PRIMARY KEY,
    product_id INT,
    name TEXT,
    image TEXT,
    price INT,
    description TEXT,
    shop_id INT);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE stock_data (id SERIAL PRIMARY KEY,
    product_id INT,
    quantity INT,
    shop_id INT);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE shop_data (id SERIAL PRIMARY KEY,
    shop_id INT, shop_name TEXT);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE order_data (id SERIAL PRIMARY KEY,
    user_id INT,
    product_id INT,
    quantity INT,
    purchase_date TIMESTAMP,
    payment_date TIMESTAMP,
    shipping_date TIMESTAMP,
    delivery_date TIMESTAMP,
    shop_id INT, price INT);
EOSQL