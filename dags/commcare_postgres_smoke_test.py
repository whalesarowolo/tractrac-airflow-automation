from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="commcare_postgres_smoke_test",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgres", "smoke-test"],
) as dag:

    SQLExecuteQueryOperator(
        task_id="select_1",
        conn_id="tractrac_postgres",
        sql="SELECT 1;",
    )