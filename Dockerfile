FROM apache/airflow:3.1.3

USER airflow

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt

# Copy DAGs (and any plugins) — make sure permissions are OK
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/