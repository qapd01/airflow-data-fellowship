"""
=== POC 06: Data Interval & Logical Date ===
📖 Slide Reference: หน้า 37 (Data Interval & Logical Date), หน้า 40-41 (Jinja Filters & Context)

แนวคิดสำคัญ: Data Interval คืออะไร?
- Airflow ไม่ได้ "run ตอนนี้แล้วดึงข้อมูลตอนนี้"
- แต่ Airflow จะ run เพื่อ process ข้อมูลของ "ช่วงเวลาที่ผ่านมา" (data interval)

ตัวอย่าง:
- DAG schedule="@daily", start_date=June 1
- DAG Run วันที่ June 2 เวลา 00:00 UTC จะ process ข้อมูลของ June 1
  - logical_date = June 1 (วันที่ข้อมูล)
  - data_interval_start = June 1 00:00
  - data_interval_end = June 2 00:00
  - actual run time = June 2 00:00 (หรือหลังจากนั้น)

สิ่งที่ต้องจำ:
- logical_date ≠ เวลาที่ DAG run จริง
- logical_date = data_interval_start (สำหรับ schedule ปกติ)
- ds = logical_date ในรูปแบบ "YYYY-MM-DD"
- ds_nodash = logical_date ในรูปแบบ "YYYYMMDD"

Scenario: pipeline ดึง log ตาม data interval
"""

import pendulum
from airflow.sdk import dag, task, get_current_context
from airflow.operators.bash import BashOperator


# ============================================================
# DAG Definition
# ============================================================
@dag(
    dag_id="poc_data_interval_logs",
    # schedule="@daily" หมายถึง run ทุกวัน เที่ยงคืน UTC
    # แต่จริงๆ มัน run เพื่อ process ข้อมูลของ "วันก่อนหน้า"
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,  # ไม่ต้อง backfill วันที่ผ่านมา
    tags=["poc", "data_interval"],
    doc_md="""
    ## POC: Data Interval & Logical Date
    สาธิตความแตกต่างระหว่าง logical_date กับเวลาจริงที่ DAG run
    """,
)
def poc_data_interval_logs():

    # ----------------------------------------------------------
    # Task 1: แสดง interval ต่างๆ จาก context
    # ----------------------------------------------------------
    @task
    def show_intervals():
        """
        ดึง context variables ที่เกี่ยวกับ data interval มาแสดง
        เพื่อให้เห็นความแตกต่างระหว่าง logical_date กับเวลาจริง
        """
        # get_current_context() ใช้ดึง context ใน @task decorator
        # context มี key ต่างๆ เช่น ds, logical_date, data_interval_start, etc.
        context = get_current_context()

        # data_interval_start: จุดเริ่มต้นของช่วงเวลาข้อมูล
        # สำหรับ @daily คือ 00:00 UTC ของวันที่ข้อมูล
        data_interval_start = context["data_interval_start"]

        # data_interval_end: จุดสิ้นสุดของช่วงเวลาข้อมูล
        # สำหรับ @daily คือ 00:00 UTC ของวันถัดไป
        data_interval_end = context["data_interval_end"]

        # logical_date: วันที่เชิง logic (= data_interval_start)
        # นี่คือ "วันที่ข้อมูล" ไม่ใช่วันที่ run จริง
        logical_date = context["logical_date"]

        # เวลาจริงที่ DAG กำลัง run อยู่ตอนนี้
        actual_now = pendulum.now("UTC")

        print("=" * 60)
        print("📅 DATA INTERVAL INFORMATION")
        print("=" * 60)
        print(f"  data_interval_start : {data_interval_start}")
        print(f"  data_interval_end   : {data_interval_end}")
        print(f"  logical_date        : {logical_date}")
        print(f"  actual run time     : {actual_now}")
        print("-" * 60)

        # แสดงความแตกต่าง: logical_date กับเวลาจริง
        # ถ้า catchup=True จะเห็นว่า logical_date อาจห่างจากเวลาจริงมาก
        diff = actual_now - logical_date
        print(f"  ⏱️  Difference (actual - logical): {diff}")
        print(f"  💡 logical_date ≠ actual run time!")
        print(f"     logical_date คือวันที่ของ 'ข้อมูล' ที่เรากำลัง process")
        print(f"     actual run time คือเวลาจริงที่ task กำลังทำงาน")
        print("=" * 60)

    # ----------------------------------------------------------
    # Task 2: BashOperator กับ Jinja template
    # ----------------------------------------------------------
    # BashOperator สามารถใช้ Jinja template ได้โดยตรง
    # {{ ds }} = logical_date ในรูปแบบ "YYYY-MM-DD"
    # {{ ds_nodash }} = logical_date ในรูปแบบ "YYYYMMDD"
    # {{ data_interval_start | ds }} = data_interval_start แปลงเป็น "YYYY-MM-DD"
    process_daily_logs = BashOperator(
        task_id="process_daily_logs",
        bash_command=(
            'echo "Processing logs for date: {{ ds }}" && '
            'echo "Interval: {{ data_interval_start | ds }} to {{ data_interval_end | ds }}" && '
            'echo "No-dash format: {{ ds_nodash }}"'
        ),
    )

    # ----------------------------------------------------------
    # Task 3: สร้างชื่อไฟล์ dynamic จาก context
    # ----------------------------------------------------------
    @task
    def generate_filename():
        """
        ใช้ context เพื่อสร้างชื่อไฟล์ที่มี date stamp
        เช่น logs_20260629.csv

        ในงานจริงจะใช้ชื่อไฟล์แบบนี้เพื่อ:
        - จัดเก็บไฟล์แยกตามวัน
        - ทำให้ idempotent (run ซ้ำได้ผลเหมือนเดิม)
        """
        context = get_current_context()

        # ดึง data_interval_start แล้วแปลงเป็น format YYYYMMDD
        # เหมือน ds_nodash แต่เราทำเองจาก data_interval_start
        ds_nodash = context["data_interval_start"].strftime("%Y%m%d")

        filename = f"logs_{ds_nodash}.csv"
        print(f"📁 Generated filename: {filename}")
        print(f"   Full path: /data/logs/{filename}")
        print(f"   💡 ชื่อไฟล์มี date stamp เพื่อให้ idempotent")
        print(f"      ถ้า run ซ้ำวันเดิม จะ overwrite ไฟล์เดิม = ผลลัพธ์เหมือนกัน")

        return filename

    # ----------------------------------------------------------
    # Dependencies: show_intervals >> process_daily_logs >> generate_filename
    # ----------------------------------------------------------
    # ลำดับ: ดูข้อมูล interval → process logs → สร้างชื่อไฟล์
    show_intervals() >> process_daily_logs >> generate_filename()


# สร้าง DAG instance — จำเป็นต้องเรียก function เพื่อให้ Airflow detect DAG
poc_data_interval_logs()
