#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE target_db;
    GRANT ALL PRIVILEGES ON DATABASE target_db TO $POSTGRES_USER;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE user_behavior_log (
    user_author_id INT,
    action VARCHAR(100),
    button_product_id INT,
    stimulus VARCHAR(100),
    component VARCHAR(100),
    text_content TEXT,
    date TIMESTAMP
);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE consumer_data (user_id INT,
    name TEXT,
    surname TEXT,
    city TEXT,
    born_date TIMESTAMP,
    register_date TIMESTAMP);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE product_data (product_id INT,
    name TEXT,
    image TEXT,
    price INT,
    description TEXT,
    shop_id INT);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE stock_data (product_id INT,
    quantity INT,
    shop_id INT);
EOSQL



psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE order_data (user_id INT,
    product_id INT,
    quantity INT,
    purchase_date TIMESTAMP,
    payment_date TIMESTAMP,
    shipping_date TIMESTAMP,
    delivery_date TIMESTAMP,
    shop_id INT, price INT);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE shop_data (shop_id INT,
     shop_name TEXT);
EOSQL



psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE audit_log (user_author_id INT,
    action TEXT,
    action_description TEXT,
    text_content TEXT,
    date TIMESTAMP);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE failure_notification_log (component TEXT,
    severity TEXT,
    message TEXT, 
    text_content TEXT, date TIMESTAMP);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-EOSQL
    CREATE TABLE debug_log (message TEXT, 
    text_content TEXT, 
    date TIMESTAMP);
EOSQL



psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE consumer_data (user_id INT,
    name TEXT,
    surname TEXT,
    city TEXT,
    born_date TIMESTAMP,
    register_date TIMESTAMP);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE product_data (product_id INT,
    name TEXT,
    image TEXT,
    price INT,
    description TEXT,
    shop_id INT);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DBget_db" <<-EOSQL
    CREATE TABLE stock_data (product_id INT,
    quantity INT,
    shop_id INT);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE shop_data (shop_id INT,
    name TEXT,
    city TEXT,
    address TEXT,
    phone TEXT);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE order_data (user_id INT,
    product_id INT,
    quantity INT,
    purchase_date TIMESTAMP,
    payment_date TIMESTAMP,
    shipping_date TIMESTAMP,
    delivery_date TIMESTAMP,
    shop_id INT, price INT);
EOSQL



psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE user_behavior_log(user_author_id INT,
    action TEXT,
    button_product_id INT,
    stimulus TEXT,
    component TEXT,
    text_content TEXT,
    date TIMESTAMP);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE audit_log (user_author_id INT,
    action TEXT,
    action_description TEXT,
    text_content TEXT,
    date TIMESTAMP);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE failure_notification_log (component TEXT,
    severity TEXT,
    message TEXT, 
    text_content TEXT, date TIMESTAMP);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE debug_log (message TEXT, 
    text_content TEXT, 
    date TIMESTAMP);
EOSQL