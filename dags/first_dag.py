from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

dag1=DAG(
    dag_id="first_day",
    start_date=datetime(2026,4,16),
    schedule_interval="@daily",
    catchup=False
)

def hello():
    print("Hello World")

def bye():
    print("Goodbye World")

task1 = PythonOperator(task_id="hello_task", python_callable=hello, dag=dag1)

task2 = PythonOperator(task_id="bye_task", python_callable=bye, dag=dag1)

task1 >> task2