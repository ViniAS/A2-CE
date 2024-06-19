#!/bin/bash

# Define PostgreSQL data directory
PGDATA="/var/lib/postgresql/16/main"

pg_ctlcluster 16 main start

until pg_isready -h localhost -p 5432
do
  echo "Waiting for PostgreSQL to start..."
  sleep 1
done

echo "PostgreSQL has been initialized."
tail -f /dev/null