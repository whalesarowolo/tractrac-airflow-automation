from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="example_hello_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,   # None or "0 2 * * *" etc.
    catchup=False,
    tags=["example"],
):
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end