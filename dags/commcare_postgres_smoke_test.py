from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


POSTGRES_CONN_ID = "tractrac_postgres"  # <-- your Airflow Connection ID


with DAG(
    dag_id="commcare_postgres_smoke_test",
    description="Smoke test: Airflow -> Postgres connection via commcare_postgres",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # trigger manually
    catchup=False,
    tags=["tractrac", "postgres", "smoke-test"],
) as dag:
    # 1) simplest possible query
    ping = PostgresOperator(
        task_id="ping_select_1",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="SELECT 1 AS ok;",
    )

    # 2) show server version in task logs
    show_version = PostgresOperator(
        task_id="show_db_version",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="SELECT version();",
    )

    # 3) OPTIONAL: create a tiny table and insert a row (comment out if you want read-only)
    create_table = PostgresOperator(
        task_id="create_smoke_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS airflow_smoke_test (
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
    )

    insert_row = PostgresOperator(
        task_id="insert_smoke_row",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="INSERT INTO airflow_smoke_test DEFAULT VALUES;",
    )

    count_rows = PostgresOperator(
        task_id="count_smoke_rows",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="SELECT COUNT(*) AS n FROM airflow_smoke_test;",
    )

    ping >> show_version >> create_table >> insert_row >> count_rows
