#!/bin/bash
set -e

source .venv/bin/activate
# Initialize the database
airflow db migrate

# Create default connections
airflow connections create-default-connections

# Create an admin user
airflow users create \
     -r Admin \
     -u admin \
     -e admin@example.com \
     -f admin \
     -l user \
     -p admin

# airflow connections dd 'clickhouse' \
if ! airflow connections get 'clickhouse' > /dev/null 2>&1; then
    airflow connections add 'clickhouse' \
        --conn-type 'sqlite' \
        --conn-host 'clickhouse-default' \
        --conn-login 'default' \
        --conn-password '' \
        --conn-port '9000'
fi

# Execute the command passed from docker-compose
exec airflow "$@"
