"""
16_depends_on_past.py — depends_on_past=True
Module 2, Phase 3: Control Flow & Branching

Task B จะรันได้ก็ต่อเมื่อ Task B ของ DAG Run รอบก่อนหน้าสำเร็จ
ถ้า Task B พัง 1 วัน → วันถัดๆ ไปทั้งหมดจะหยุดชะงัก (Block)
"""
from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="16_depends_on_past_demo",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=True,  # สำคัญ: ต้องตั้งเป็น True เพื่อจำลองสถานการณ์การรันย้อนหลัง
)
def compare_depends_on_past_demo():

    # Task A: ไม่มี depends_on_past (ทำงานอิสระ)
    @task(task_id="task_A_independent")
    def task_a():
        print("Task A รันได้ตามปกติ ไม่สนใจอดีต")

    # Task B: มี depends_on_past=True (จดจำอดีต)
    @task(
        task_id="task_B_dependent",
        depends_on_past=True
    )
    def task_b(ds=None):
        # จำลองให้พังเฉพาะวันที่ 2026-06-01 (วันแรก)
        if ds == '2026-06-01':
            print("Task B กำลังจะพังในวันที่ 2026-06-01...")
            raise Exception("เกิดข้อผิดพลาดในรอบแรก!")

        print(f"Task B รันสำเร็จในวันที่ {ds}")

    # กำหนดลำดับงาน
    task_a() >> task_b()


compare_depends_on_past_demo()
