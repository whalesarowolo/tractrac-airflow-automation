FROM apache/airflow:3.1.3

# Switch to root to install packages
USER root

# Copy the requirements file into the image
COPY requirements-airflow.txt /requirements-airflow.txt

# Install Python packages (Airflow providers and extras)
RUN pip install --no-cache-dir -r /requirements-airflow.txt

# Copy DAGs from repo into the standard Airflow DAGs folder
COPY dags /opt/airflow/dags

# Switch back to airflow user (best practice)
USER airflow