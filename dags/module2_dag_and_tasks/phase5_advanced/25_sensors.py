"""
25_sensors.py — Sensors (FileSensor)
Module 2, Phase 5: Advanced Interactions

Sensors เฝ้ารอให้บางสิ่งเกิดขึ้น ก่อนที่ Task ถัดไปจะทำงาน
- mode="poke": จอง Worker slot ตลอดเวลา
- mode="reschedule": ปล่อย Worker ระหว่างรอ

วิธีทดสอบ:
  1. Trigger DAG จาก Airflow UI
  2. Sensor จะค้างรอ...
  3. รัน `make sensor` จาก host (สร้างไฟล์ logs/data/data_ready.csv)
  4. Sensor จะ detect แล้ว task ถัดไปจะทำงานต่อ
"""
from airflow.sdk import dag
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime


@dag(
    dag_id="25_advanced_sensor_config_demo",
    start_date=datetime(2026, 6, 6),
    schedule=None,
    catchup=False,
    tags=["sensor"],
)
def advance_sensor_config_demo():

    # Step 1: Sensor
    wait_for_file = FileSensor(
        task_id="wait_for_critical_file",
        filepath="/opt/airflow/logs/data/data_ready.csv",

        # --- พารามิเตอร์ของ BaseSensorOperator ---
        mode="reschedule",          # ประหยัด Worker เพราะรอนาน
        poke_interval=10,           # ตรวจสอบทุก 10 วินาที (ลดลงเพื่อ demo ให้เร็ว)
        timeout=3600,               # ยอมให้รอนานสุด 1 ชั่วโมง

        soft_fail=True,             # ถ้าเกิน 1 ชม. ไม่ให้งานพัง (FAILED) แต่ให้ข้ามไป (SKIPPED)

        exponential_backoff=True,   # ช่วงแรกเช็คถี่ๆ ถ้ายังไม่มาให้ค่อยๆ ยืดเวลาเช็คออก
        max_wait=300                # รอนานสุดระหว่างรอบเช็คไม่เกิน 5 นาที
    )

    # Step 2: Read file
    read_file = BashOperator(
        task_id="read_file_content",
        bash_command="echo '📄 เนื้อหาในไฟล์:' && cat /opt/airflow/logs/data/data_ready.csv",
    )

    # Step 3: Delete file for test again
    cleanup_file = BashOperator(
        task_id="cleanup_file",
        bash_command="rm -f /opt/airflow/logs/data/data_ready.csv && echo '🗑️ ลบไฟล์เรียบร้อย พร้อมทดสอบรอบใหม่'",
    )

    # Dependencies: wait_for_file → read_file → cleanup_file
    wait_for_file >> read_file >> cleanup_file

advance_sensor_config_demo()
