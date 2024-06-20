# Docker network
docker network create net

# PostgreSQL
docker run --name psql_container -d --net net -p 5432:5432 -d psql

# Historico Mock
docker run  --name historico_container --net net -d historico

# Loja Mock
docker run --name loja_container --net net -d loja

# Webhook
docker run --name webhook_container --net net -d webhook

# Dashboard 
docker run --name dash_container --net net -d dash

