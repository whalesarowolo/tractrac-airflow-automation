FROM apache/airflow:3.1.3

# (Optional) if you need OS-level deps, install them as root here
USER root
# RUN apt-get update && apt-get install -y build-essential && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements-airflow.txt /requirements-airflow.txt

# Install python packages as the airflow user (this is the key fix)
USER airflow
RUN pip install --no-cache-dir -r /requirements-airflow.txt

# Copy DAGs (and any plugins) — make sure permissions are OK
COPY dags/ /opt/airflow/dags/
# COPY plugins/ /opt/airflow/plugins/