from airflow.sdk import dag, task
from datetime import datetime


@dag(dag_id='30_xcoms_modern', start_date=datetime(2026, 6, 6), schedule=None, catchup=False)
def taskflow_xcom_example():


    @task
    def push_data():
        return "This is message from  Task-1"


    @task
    def pull_data(data_from_push):
        print(f"Message recieve: {data_from_push}")


    my_data = push_data()
    pull_data(my_data)


taskflow_xcom_example()
