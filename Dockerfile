FROM apache/airflow:2.0.0

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    # this is needed for requirements packages
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /application
RUN chown -R airflow: /application
COPY ./source/dags /opt/airflow/dags
COPY ./setup/config/airflow.cfg /opt/airflow/
COPY ./source /application/source

USER airflow
COPY source/requirements.txt /opt/airflow/requirements.txt
RUN pip install -r /opt/airflow/requirements.txt
