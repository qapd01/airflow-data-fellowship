"""
24_context_inspector.py — Context Inspector
Module 2, Phase 5: Advanced Interactions

พิมพ์ข้อมูลภายใน context ทั้งหมด เพื่อให้เห็นว่า
Airflow ให้ข้อมูลอะไรบ้างขณะรัน Task
"""
from airflow.sdk import dag, task
from datetime import datetime
import pprint


@dag(
    dag_id="24_context_inspector_dag",
    start_date=datetime(2026, 6, 6),
    schedule=None,
    catchup=False,
    tags=["context"],
)
def context_inspector_dag():

    @task
    def inspect_context(**context):
        # สร้าง object สำหรับพิมพ์ให้สวยงามและอ่านง่าย
        pp = pprint.PrettyPrinter(indent=2)

        print("--- ข้อมูลภายใน Context ---")

        print(f"1. วันที่ของ Data Interval (ds): {context.get('ds')}")
        print(f"2. ชื่อ DAG ID: {context.get('dag').dag_id}")
        print(f"3. ชื่อ Task ID: {context.get('task').task_id}")
        print(f"4. รายการ Key ทั้งหมดใน Context:")

        # แสดงรายการ Key ทั้งหมดที่มีให้ใช้
        pp.pprint(list(context.keys()))

    inspect_context()


context_inspector_dag()
