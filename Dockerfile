FROM puckel/docker-airflow:1.10.9

USER root
RUN set -ex \
    && buildDeps='python-dev' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
    $buildDeps \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base

RUN mkdir /application
RUN chown -R airflow: /application

COPY source/requirements.txt /usr/local/airflow/requirements.txt
RUN pip install -r /usr/local/airflow/requirements.txt

USER airflow

# Create log directory
RUN mkdir /usr/local/airflow/logs
COPY ./source/dags /usr/local/airflow/dags
COPY ./setup/config/airflow.cfg /usr/local/airflow/
COPY ./source /application/source
