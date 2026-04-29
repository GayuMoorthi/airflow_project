from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import os
import json


def fetch_api(ti):
    url = ""
    response=requests.get(url)
    data = response.json()

    current_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"posts_{current_ts}.json"

    folder_path = "/tmp/airflow_api_data"
    os.makeddirs(folder_path, exist_ok=True)
    full_path = f"{folder_path}/{file_name}"

    with open(full_path, "w") as f:
        json.dump(data, f)

    print("Completed tas")

    ti.xcom_push(key="file_path", value=full_path)

def process_data(ti):
    file_path = ti.xcom_pull(key= 'file_path', task_ids='fetch_api_task')
    print(file_path)

    with open(file_path, "r") as f:
        data = json.load(f)
    print("Total records from API:", len(data))


dag1 = DAG(dag_id="dag1", start_date=datetime(2026, 4, 16), catchup=False)
fetch_task = PythonOperator(task_id="fetch_api_task", python_callable=fetch_api, dag=dag1)

process_task = PythonOperator(task_id="process_data_task", python_callable=process_data, dag=dag1)

fetch_task >> process_task
