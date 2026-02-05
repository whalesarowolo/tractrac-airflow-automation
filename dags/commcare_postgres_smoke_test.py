from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CONN_ID = "tractrac_postgres"  # your Airflow connection id

with DAG(
    dag_id="commcare_postgres_smoke_test",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tractrac", "postgres", "smoke-test"],
) as dag:

    ping = SQLExecuteQueryOperator(
        task_id="ping_select_1",
        conn_id=CONN_ID,
        sql="SELECT 1 AS ok;",
    )

    version = SQLExecuteQueryOperator(
        task_id="show_db_version",
        conn_id=CONN_ID,
        sql="SELECT version();",
    )

    # Optional write test: comment these 3 tasks out if you want read-only
    create_table = SQLExecuteQueryOperator(
        task_id="create_smoke_table",
        conn_id=CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS airflow_smoke_test (
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
    )

    insert_row = SQLExecuteQueryOperator(
        task_id="insert_smoke_row",
        conn_id=CONN_ID,
        sql="INSERT INTO airflow_smoke_test DEFAULT VALUES;",
    )

    count_rows = SQLExecuteQueryOperator(
        task_id="count_smoke_rows",
        conn_id=CONN_ID,
        sql="SELECT COUNT(*) AS n FROM airflow_smoke_test;",
    )

    ping >> version >> create_table >> insert_row >> count_rows
