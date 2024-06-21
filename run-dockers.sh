# Docker network
docker network create net

# PostgreSQL
docker run --name psql -d --net net -p 5432:5432 -d psql

# Broker
docker run --name broker --net net -p 5672:5672 -p 15672:15672 -d broker

# Queue
docker run --name queue --net net -d queue

# Historico Mock
docker run  --name historico --net net -d historico

# Loja Mock
docker run --name loja --net net -d loja

# Webhook
docker run --name webhook --net net -p 55555:55555 -d webhook 

# Web Gen
docker run --name web_gen --net net -d web_gen

# Dashboard 
docker run --name dash --net net -d dash

