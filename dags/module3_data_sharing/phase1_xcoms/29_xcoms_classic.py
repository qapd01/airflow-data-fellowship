from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id='29_xcoms_classic', 
    start_date=datetime(2026, 6, 6), 
    schedule=None, 
    catchup=False
)
def taskflow_with_classic_xcom():


    @task
    def push_task(ti=None):
        ti.xcom_push(key='my_key', value='นี่คือข้อมูลที่ push มาแบบท่าเก่า')
        print("Push ข้อมูลเรียบร้อยแล้ว!")


    @task
    def pull_task(ti=None):
        value = ti.xcom_pull(task_ids='push_task', key='my_key')
        print(f"ข้อมูลที่ดึงมาได้คือ: {value}")


    push_task() >> pull_task()


taskflow_with_classic_xcom()
