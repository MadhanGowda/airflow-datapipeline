# Airflow Datapipeline

## Setup

-------------
Create .env file inside source folder.

Refer/Duplicate env.example

``` bash
# build the docker image
docker-compose build

# Bring the containers up
docker-compose up -d

# Create Admin user
docker-compose run worker airflow users create --role Admin --username admin --email example@example.com --firstname First --lastname Last --password admin
```

## URLS - local
-------------

- Airflow: localhost:8080
- Flower: localhost:5555

