docker-compose build
AIRFLOW_UID=1000 docker-compose up -d
AIRFLOW_UID=1000 docker-compose down -v


## variables
support_email
dottami@gmail.com

web_api_key
https://data-api.ecb.europa.eu/service/data/EXR/M.USD.EUR.SP00.A?format=csvdata

query_interval
{"start_date": "2024-09-23", "end_date": "2024-09-24"}

## connection
connection Id: postgres_conn
Host: postgres
Database: airflow
Login airflow
Password: airflow