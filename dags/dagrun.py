from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def task_a(**context):
    val = 42
    context['ti'].xcom_push(key='my_key', value=val)

def task_b(**context):
    retrieved_val = context['ti'].xcom_pull(task_ids='task_a', key='my_key')
    print(f"Retrieved value from XCom: {retrieved_val}")

dag = DAG(
    dag_id="xcom_dag",
    start_date=datetime(2026, 4, 29),
    schedule_interval="@daily",
    catchup=False
)

task1 = PythonOperator(
    task_id="task_a",
    python_callable=task_a,
    dag=dag
)

task2 = PythonOperator(
    task_id="task_b",
    python_callable=task_b,
    dag=dag
)

task1 >> task2