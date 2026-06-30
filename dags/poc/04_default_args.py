"""
04_default_args.py — default_args และการ Override ระดับ Task
=============================================================
📖 Slide Reference: หน้า 33 (Default Arguments)

ไฟล์นี้สาธิตการใช้ `default_args` ใน DAG:
- default_args คือ dictionary ที่กำหนดค่าเริ่มต้นให้ทุก task ใน DAG
- task แต่ละตัวสามารถ override ค่าจาก default_args ได้

💡 ลำดับความสำคัญ (Override Precedence):
   1. Task-level parameter    ← สูงสุด (ชนะทุกอย่าง)
   2. default_args             ← ค่า default ของ DAG
   3. Airflow global config    ← ต่ำสุด

ตัวอย่าง:
   default_args = {"retries": 3}
   @task(retries=5)  ← task นี้จะ retry 5 ครั้ง ไม่ใช่ 3 ครั้ง
                       เพราะ task-level override ชนะ default_args

Scenario: Data Warehouse ETL Pipeline
- extract จาก API และ Database
- transform ข้อมูล (task นี้ override retries เป็น 5)
- load เข้า warehouse
"""

from datetime import timedelta

from airflow.sdk import dag, task
import pendulum


# ============================================================================
# default_args — ค่าเริ่มต้นที่ทุก task จะได้รับ
# ============================================================================
# ค่าเหล่านี้จะถูกส่งให้ทุก task ใน DAG โดยอัตโนมัติ
# เหมาะสำหรับค่าที่ task ส่วนใหญ่ใช้เหมือนกัน เช่น:
#   - owner: ชื่อทีมหรือคนที่รับผิดชอบ
#   - retries: จำนวนครั้งที่ retry เมื่อ task fail
#   - retry_delay: เวลารอก่อน retry
#   - email_on_failure: ส่ง email เมื่อ fail
#   - execution_timeout: timeout ของ task

default_args = {
    # owner — ระบุทีมหรือคนที่รับผิดชอบ DAG นี้
    # ใช้สำหรับ filtering ใน Airflow UI และ alerting
    "owner": "data-team",

    # retries — จำนวนครั้งที่ retry เมื่อ task fail
    # ค่า default ที่เหมาะสมคือ 2-3 ครั้ง สำหรับ task ทั่วไป
    "retries": 3,

    # retry_delay — เวลารอก่อน retry แต่ละครั้ง
    # timedelta(minutes=5) = รอ 5 นาที ก่อน retry
    # ช่วยให้ระบบปลายทาง recover ก่อน retry
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="poc_default_args_warehouse",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    # ส่ง default_args เข้าไปใน @dag decorator
    # ทุก task จะได้รับค่าเหล่านี้เป็น default
    default_args=default_args,
    tags=["poc", "default_args"],
    doc_md="""
    ### Data Warehouse ETL Pipeline
    สาธิต `default_args` และการ override ระดับ task

    **Override Precedence:**
    Task-level > default_args > Airflow config
    """,
)
def warehouse_etl_pipeline():
    """
    Pipeline ETL สำหรับ Data Warehouse
    แสดงการใช้ default_args และ task-level override
    """

    # ====================================================================
    # Task 1: extract_from_api — ใช้ default_args ตามปกติ
    # ====================================================================
    # task นี้จะได้รับ:
    #   owner="data-team"    ← จาก default_args
    #   retries=3            ← จาก default_args
    #   retry_delay=5 min    ← จาก default_args

    @task
    def extract_from_api():
        """
        ดึงข้อมูลจาก REST API
        ใช้ default_args ทั้งหมด ไม่ override อะไร
        → retries=3, retry_delay=5min (จาก default_args)
        """
        print("🌐 Extracting data from REST API...")
        print("   Endpoint: https://api.example.com/v1/transactions")
        print("   Records fetched: 5,000")
        return {"source": "api", "records": 5000}

    # ====================================================================
    # Task 2: extract_from_db — ใช้ default_args ตามปกติ
    # ====================================================================
    # เหมือน task ข้างบน — ใช้ค่า default ทั้งหมด

    @task
    def extract_from_db():
        """
        ดึงข้อมูลจาก Database
        ใช้ default_args ทั้งหมด ไม่ override อะไร
        → retries=3, retry_delay=5min (จาก default_args)
        """
        print("🗄️ Extracting data from PostgreSQL database...")
        print("   Query: SELECT * FROM orders WHERE date = '{{ ds }}'")
        print("   Records fetched: 12,000")
        return {"source": "database", "records": 12000}

    # ====================================================================
    # Task 3: transform_data — ⚠️ Override retries เป็น 5
    # ====================================================================
    # task นี้ override retries จาก 3 → 5
    # เพราะ transformation มักใช้ memory/CPU มาก
    # อาจ fail จาก resource contention จึง retry มากกว่า task อื่น
    #
    # 💡 Override Precedence:
    #   retries → ใช้ค่า 5 จาก @task(retries=5) ไม่ใช่ 3 จาก default_args
    #   owner   → ยังคงเป็น "data-team" จาก default_args (ไม่ได้ override)
    #   retry_delay → ยังคงเป็น 5 min จาก default_args (ไม่ได้ override)

    @task(retries=5)  # ← Override! ใช้ 5 แทน 3 จาก default_args
    def transform_data(api_data: dict, db_data: dict):
        """
        แปลงและรวมข้อมูลจากหลาย sources

        ⚠️ task นี้ override retries=5 (default_args กำหนดไว้ 3)
        เหตุผล: transformation ใช้ resource มาก มีโอกาส fail สูงกว่า
        """
        total = api_data["records"] + db_data["records"]
        print(f"🔄 Transforming data from {api_data['source']} + {db_data['source']}...")
        print(f"   Total input records: {total:,}")
        print(f"   Cleaning nulls and duplicates...")
        print(f"   Calculating aggregates...")

        # สังเกต: retries ของ task นี้คือ 5 (ไม่ใช่ 3)
        # เพราะ @task(retries=5) override ค่าจาก default_args
        print(f"   ℹ️ This task has retries=5 (overridden from default_args retries=3)")

        return {"records_transformed": total, "status": "success"}

    # ====================================================================
    # Task 4: load_to_warehouse — ใช้ default_args ตามปกติ
    # ====================================================================

    @task
    def load_to_warehouse(transformed: dict):
        """
        โหลดข้อมูลที่แปลงแล้วเข้า Data Warehouse
        ใช้ default_args ทั้งหมด ไม่ override อะไร
        → retries=3, retry_delay=5min
        """
        print(f"📤 Loading {transformed['records_transformed']:,} records to warehouse...")
        print(f"   Target: fact_transactions table")
        print(f"   Status: {transformed['status']}")
        print(f"   ✅ Load complete!")

    # ====================================================================
    # Dependencies: [extract_api, extract_db] >> transform >> load
    # ====================================================================
    # Fan-in pattern: ดึงข้อมูลจาก 2 sources พร้อมกัน
    # แล้วรวมกันใน transform แล้ว load

    api_data = extract_from_api()
    db_data = extract_from_db()

    # ส่ง output จาก 2 extract tasks เข้า transform
    # Airflow สร้าง dependency อัตโนมัติจาก function call
    transformed = transform_data(api_data, db_data)
    load_to_warehouse(transformed)


# ⚠️ สำคัญ: ต้อง call function เพื่อ instantiate DAG
warehouse_etl_pipeline()


# ============================================================================
# 📝 สรุป default_args ที่ใช้บ่อย:
# ============================================================================
#
# | Parameter          | Type          | ตัวอย่าง                        | คำอธิบาย                    |
# |-------------------|---------------|--------------------------------|---------------------------|
# | owner             | str           | "data-team"                    | เจ้าของ/ทีมที่รับผิดชอบ      |
# | retries           | int           | 3                              | จำนวนครั้ง retry             |
# | retry_delay       | timedelta     | timedelta(minutes=5)           | เวลารอก่อน retry            |
# | email_on_failure  | bool          | True                           | ส่ง email เมื่อ fail         |
# | email_on_retry    | bool          | False                          | ส่ง email เมื่อ retry        |
# | execution_timeout | timedelta     | timedelta(hours=1)             | timeout ของ task            |
# | depends_on_past   | bool          | False                          | ต้องรอ run ก่อนหน้าเสร็จมั้ย  |
# | sla               | timedelta     | timedelta(hours=2)             | SLA deadline               |
