from datetime import datetime
from airflow import DAG
from airflow.decorators import task

import requests
import pandas as pd

ODATA_URL = "https://www.commcarehq.org/a/tractrac-issam-project/api/odata/forms/v1/9f29f0b692c501e1d1c87daf091b4fb7/feed"

with DAG(
    dag_id="odata_ingest_dag",
    start_date=datetime(2025, 1, 1),
    schedule="0 3 * * *",  # daily 3AM
    catchup=False,
    tags=["odata", "tractrac"],):

    @task
    def fetch_and_print():
        resp = requests.get(ODATA_URL)
        resp.raise_for_status()
        data = resp.json()
        df = pd.json_normalize(data["value"])
        print(df.head())

    fetch_and_print()
