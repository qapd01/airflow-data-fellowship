"""
26_dag_trigger.py — TriggerDagRunOperator (DAG A → DAG B)
Module 2, Phase 5: Advanced Interactions

DAG A ทำงานเสร็จแล้วสั่งให้ DAG B เริ่มรัน
ใช้ TriggerDagRunOperator
"""
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


@dag(
    dag_id="26_dag_a_trigger",
    start_date=datetime(2026, 6, 6),
    schedule="@daily",
    catchup=False,
    tags=["dag_dependency"],
)
def dag_a_trigger():

    @task
    def process_something():
        print("DAG A กำลังทำงาน...")
        return "Task Success"

    # ใช้ TriggerDagRunOperator เพื่อสั่งรัน DAG B
    trigger_b = TriggerDagRunOperator(
        task_id="trigger_dag_b",
        trigger_dag_id="dag_b_target",  # ต้องตรงกับ dag_id ของไฟล์ที่ 2
        wait_for_completion=False       # ถ้า True: DAG A จะรอจนกว่า DAG B จะรันเสร็จ
    )

    process_something() >> trigger_b


dag_a_trigger()
