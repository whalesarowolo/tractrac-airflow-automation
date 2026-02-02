FROM apache/airflow:3.1.3
ARG AIRFLOW_VERSION=3.1.3
ARG PYTHON_VERSION=3.12

USER root

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt \
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER airflow
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/