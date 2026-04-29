from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

def download():
    print("Downloading data...")

def validate():
    print("Validating data...")

def clean():
    print("Cleaning data")

def transform():
    print("Transforming data...")

def store():
    print("Storing data")

def notify():
    print("Notifying stakeholders...")

#run task as group once success then run task6

with DAG(dag_id="task_group_dag", start_date=datetime(2026,4,23), catchup=False) as dag:
    with TaskGroup(group_id="task_group") as task_group:
        task1 = PythonOperator(task_id="download_task", python_callable=download)
        task2= PythonOperator(task_id="validate_task", pytho_callable=validate)
        task3 = PythonOperator(task_id="clean_task", python_callable=clean)
        task4 = PythonOperator(task_id="transform_task", python_callable=transform)
        task5 = PythonOperator(task_id="store_task", python_callable=store)

        task1 >> task2 >> task3 >> task4 >> task5

    task6 = PythonOperator(task_id="notify_task", python_callable=notify)
    task_group >> task6


#-----------------------------------------------------------------------------
#                               ( OR )
#-----------------------------------------------------------------------------

#run task in sequence order

with DAG(dag_id="task_group_dag", start_date=datetime(2026,4,23), catchup=False) as dag:
    with TaskGroup(group_id="task_group") as task_group:
        task1 = PythonOperator(task_id="download_task", pythonCallable=download)
        task2= PythonOperator(task_id="validate_task", pythonCallable=validate)
        task3 = PythonOperator(task_id="clean_task", pythonCallable=clean)
        task4 = PythonOperator(task_id="transform_task", pythonCallable=transform)
        task5 = PythonOperator(task_id="store_task", pythonCallable=store)

    task6 = PythonOperator(task_id="notify_task", pythonCallable=notify)
    task_group >> task6

#-----------------------------------------------------------------------------
