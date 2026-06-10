"""
27_dag_target.py — DAG B (ถูก Trigger จาก DAG A)
Module 2, Phase 5: Advanced Interactions

DAG นี้ไม่มี schedule (None) เพราะจะถูกสั่งรันจาก DAG A
ผ่าน TriggerDagRunOperator
"""
from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="27_dag_b_target",  # ชื่อนี้ต้องตรงกับ trigger_dag_id ใน DAG A
    start_date=datetime(2026, 6, 6),
    schedule=None,           # ปกติ DAG ที่ถูก Trigger มักตั้ง schedule=None
    catchup=False,
    tags=["dag_dependency"],
)
def dag_b_target():

    @task
    def run_target_task():
        print("DAG B ถูกสั่งรันสำเร็จแล้ว!")

    run_target_task()


dag_b_target()
