"""
=== POC 09: Catchup & Backfill ===
📖 Slide Reference: หน้า 10-16 (Idempotency & Designing for Failure), หน้า 24 (Catchup & Backfill)

============================================================
🔑 CATCHUP & BACKFILL — แนวคิดสำคัญ
============================================================

1. catchup=True คืออะไร?
   - เมื่อ DAG มี start_date ในอดีต และ catchup=True
   - Airflow จะสร้าง DAG Runs สำหรับทุก interval ที่ "ยังไม่ได้ run"
   - ตัวอย่าง: start_date=June 26, วันนี้=June 29, schedule=@daily
     → Airflow จะสร้าง DAG Runs สำหรับ:
       - June 26 (data interval: June 26-27)
       - June 27 (data interval: June 27-28)
       - June 28 (data interval: June 28-29)
     → รวม 3 DAG Runs ที่ run ทันที!

2. catchup=False คืออะไร?
   - Airflow จะ run แค่ interval ล่าสุดเท่านั้น
   - ไม่สนใจ interval ที่ผ่านไปแล้ว
   - ใช้เมื่อไม่ต้องการ backfill อัตโนมัติ

3. Manual Backfill คืออะไร?
   - ใช้ CLI เพื่อ run DAG ย้อนหลังสำหรับ date range ที่ต้องการ
   - คำสั่ง: airflow dags backfill -s 2026-06-01 -e 2026-06-15 poc_catchup_backfill_demo
   - ทำได้แม้ catchup=False

4. ทำไมต้อง Idempotent Design?
   - Idempotent = "run กี่ครั้งก็ได้ผลเหมือนกัน"
   - สำคัญมากสำหรับ catchup/backfill เพราะ:
     a) DAG Run อาจถูก run ซ้ำ (retry, rerun)
     b) Backfill จะ run อีกรอบสำหรับ interval เดิม
   - Pattern: DELETE then INSERT (ลบข้อมูลเก่าก่อนใส่ใหม่)
   - ถ้าไม่ idempotent → ข้อมูลซ้ำ (duplicate data)!

============================================================

Scenario: Daily pipeline ที่ backfill ย้อนหลัง 3 วัน
"""

import pendulum
from airflow.sdk import dag, task, get_current_context


# ============================================================
# DAG Definition
# ============================================================
@dag(
    dag_id="poc_catchup_backfill_demo",
    schedule="@daily",
    # start_date ย้อนหลัง 3 วัน — Airflow จะ backfill ให้อัตโนมัติ
    start_date=pendulum.datetime(2026, 6, 26, tz="UTC"),
    # ⭐ catchup=True: นี่คือ key setting!
    # Airflow จะสร้าง DAG Runs สำหรับทุก interval ที่ยังไม่ได้ run
    catchup=True,
    tags=["poc", "catchup", "backfill"],
    doc_md="""
    ## POC: Catchup & Backfill
    - สาธิต catchup=True กับ idempotent design
    - DAG นี้จะ backfill ย้อนหลังตั้งแต่ start_date ถึงวันนี้
    - สังเกตว่า Airflow สร้างหลาย DAG Runs พร้อมกัน
    """,
)
def poc_catchup_backfill_demo():

    # ----------------------------------------------------------
    # Task 1: Extract — ดึงข้อมูลรายวัน
    # ----------------------------------------------------------
    @task
    def extract_daily_data():
        """
        ดึงข้อมูลตาม data interval

        สังเกต: เมื่อ catchup=True
        - Task นี้จะถูก run หลายครั้ง (1 ครั้งต่อ 1 interval)
        - แต่ละครั้ง ds จะต่างกัน (June 26, June 27, June 28, ...)
        - ทำให้เราได้ข้อมูลครบทุกวัน
        """
        context = get_current_context()
        ds = context["ds"]

        print(f"📥 Extracting data for date: {ds}")
        print(f"   Data interval: {context['data_interval_start']} to {context['data_interval_end']}")

        # Simulate idempotent extraction
        # ในงานจริงจะดึงข้อมูลจาก API/DB ตาม date filter
        # เช่น: SELECT * FROM events WHERE date = '{ds}'
        data = {"date": ds, "records": 100, "source": "api"}
        print(f"   ✅ Extracted {data['records']} records from {data['source']}")

        return data

    # ----------------------------------------------------------
    # Task 2: Transform — แปลงข้อมูล
    # ----------------------------------------------------------
    @task
    def transform_data(data: dict):
        """
        แปลงข้อมูลที่ดึงมา

        💡 Task นี้รับ data จาก extract_daily_data ผ่าน XCom อัตโนมัติ
        (TaskFlow API จัดการ XCom push/pull ให้)
        """
        print(f"🔄 Transforming data for date: {data['date']}")
        print(f"   Input records: {data['records']}")

        # Simulate transformation
        transformed = {
            "date": data["date"],
            "records": data["records"],
            "processed_records": int(data["records"] * 0.95),  # 95% pass validation
            "invalid_records": int(data["records"] * 0.05),    # 5% fail validation
            "source": data["source"],
            "status": "transformed",
        }

        print(f"   ✅ Valid records: {transformed['processed_records']}")
        print(f"   ⚠️ Invalid records: {transformed['invalid_records']}")

        return transformed

    # ----------------------------------------------------------
    # Task 3: Load — โหลดข้อมูลแบบ Idempotent
    # ----------------------------------------------------------
    @task
    def load_data(data: dict):
        """
        โหลดข้อมูลลง database แบบ Idempotent

        ⭐ Idempotent Load Pattern: DELETE then INSERT
        - ลบข้อมูลเก่าของวันนั้นก่อน
        - แล้วค่อยใส่ข้อมูลใหม่
        - ผลลัพธ์: run 1 ครั้งหรือ 100 ครั้ง ได้ผลเหมือนกัน!

        ทำไมต้อง Idempotent?
        - catchup=True อาจ run ซ้ำ interval เดิม
        - retry อาจ run task ซ้ำ
        - manual rerun/backfill อาจ run ซ้ำ
        - ถ้าแค่ INSERT → ข้อมูลซ้ำ (duplicate)!
        """
        print("=" * 60)
        print(f"📤 LOADING DATA — Idempotent Pattern")
        print("=" * 60)

        # Idempotent load pattern: DELETE then INSERT
        print(f"🔄 Idempotent load for {data['date']}:")
        print(f"   Step 1: DELETE FROM daily_report WHERE date = '{data['date']}'")
        print(f"   Step 2: INSERT INTO daily_report VALUES ('{data['date']}', ...)")
        print(f"   ✅ Result is same whether run 1x or 100x!")
        print()
        print(f"   📊 Loaded {data['processed_records']} records for {data['date']}")
        print(f"   ❌ Skipped {data['invalid_records']} invalid records")
        print()
        print("💡 Alternative idempotent patterns:")
        print("   - UPSERT (INSERT ON CONFLICT UPDATE)")
        print("   - MERGE statement")
        print("   - Write to partitioned table (overwrite partition)")
        print("   - Write to file with date-based naming (overwrite file)")
        print("=" * 60)

    # ----------------------------------------------------------
    # Dependencies: extract >> transform >> load (ETL pipeline)
    # ----------------------------------------------------------
    extracted = extract_daily_data()
    transformed = transform_data(extracted)
    load_data(transformed)


# สร้าง DAG instance — จำเป็นต้องเรียก function เพื่อให้ Airflow detect DAG
poc_catchup_backfill_demo()
