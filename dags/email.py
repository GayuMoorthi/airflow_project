from airflow import DAG
from airflow.operators.python import  PythonOperator
from datetime import datetime, timedelta

def sucess():
    print("Successfully passed")

def failed():
    raise Exception("Task Failed")

default_args = {
    "owner" : "Gayathri",
    "email" : ["gayumoorthi77@gmail.com"],
    "email_on_failure" : True, #true=> email will be sent when task is failed.. false= > email will not be sent when task is failed
    "email_on_retry" : True, #true=> email will be sent when task is retried at first time.. false= > email will not be sent when task is retried at first time
    "retries" : 1, #number of times the task will be retried when it is failed
    "retry_delay" : timedelta(minutes=5) #time duration between two retries
}

with DAG(dag_id="email_alert", default_args=default_args, 
         start_date=datetime(2026,4,23), schedule_interval="@daily", catchup=False) as dag:
    task1 = PythonOperator(task_id="success_task", python_callable=sucess)
    task2 = PythonOperator(task_id="failed_task", python_callable=failed)
    
    task1 >> task2
 