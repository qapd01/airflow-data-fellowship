"""
=== POC 07: Connections & Hooks ===
📖 Slide Reference: หน้า 38 (Connections & Hooks)

แนวคิดสำคัญ:

1. Connection คืออะไร?
   - Connection คือ "ข้อมูลการเชื่อมต่อ" ที่เก็บไว้ใน Airflow
   - เช่น host, port, username, password ของ database หรือ API
   - จัดการผ่าน Airflow UI > Admin > Connections
   - ข้อดี: ไม่ต้อง hardcode credentials ใน code

2. Hook คืออะไร?
   - Hook คือ "interface" ที่ wrap connection ให้ใช้งานง่าย
   - เช่น S3Hook จะจัดการ connect/disconnect กับ S3 ให้เรา
   - แทนที่จะเขียน boto3 code เอง ใช้ S3Hook แทน
   - Hook ช่วยเรื่อง: connection management, error handling, retry logic

3. ความสัมพันธ์:
   Connection (ข้อมูล) → Hook (interface) → Operator (action)
   เช่น: minio_default → S3Hook → S3ToLocalFilesystem

Scenario: ดึง connection info จาก Airflow connections ที่มีอยู่ในระบบ
"""

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook


# ============================================================
# DAG Definition
# ============================================================
@dag(
    dag_id="poc_connections_hooks_demo",
    schedule=None,  # Trigger manually เท่านั้น
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["poc", "connections", "hooks"],
    doc_md="""
    ## POC: Connections & Hooks
    สาธิตการใช้ Airflow Connections และ Hooks
    - ดึงข้อมูล connection จาก Airflow metadata
    - แสดงวิธีใช้ Hook (concept)
    - Best practices สำหรับการจัดการ connections
    """,
)
def poc_connections_hooks_demo():

    # ----------------------------------------------------------
    # Task 1: ดึงข้อมูล connection ที่มีอยู่ในระบบ
    # ----------------------------------------------------------
    @task
    def list_available_connections():
        """
        ใช้ BaseHook.get_connection() เพื่อดึงข้อมูล connection
        BaseHook เป็น class แม่ของ Hook ทุกตัว มี method สำหรับจัดการ connection

        ⚠️ ต้องสร้าง connection ใน Airflow UI ก่อน ไม่งั้นจะ error
        """
        # ลอง get connection "minio_default" ที่ควรมีอยู่ในระบบ
        # (ตาม AGENTS.md: minio_default — S3-compatible conn to MinIO)
        try:
            conn = BaseHook.get_connection("minio_default")
            print("=" * 60)
            print("🔌 CONNECTION: minio_default")
            print("=" * 60)
            print(f"  Connection found: {conn.conn_id}")
            print(f"  Host: {conn.host}")
            print(f"  Port: {conn.port}")
            print(f"  Schema: {conn.schema}")
            print(f"  Login: {conn.login}")
            # Never print password in production!
            # ⚠️ อย่า print password ใน production เด็ดขาด!
            print(f"  Connection Type: {conn.conn_type}")
            print("=" * 60)
        except Exception as e:
            print(f"⚠️ Connection 'minio_default' not found: {e}")
            print("Please create the connection in Airflow UI > Admin > Connections")

        # ลอง get connection "clickhouse_conn" ด้วย
        try:
            conn = BaseHook.get_connection("clickhouse_conn")
            print()
            print("=" * 60)
            print("🔌 CONNECTION: clickhouse_conn")
            print("=" * 60)
            print(f"  Connection found: {conn.conn_id}")
            print(f"  Host: {conn.host}")
            print(f"  Port: {conn.port}")
            print(f"  Schema: {conn.schema}")
            print(f"  Login: {conn.login}")
            print(f"  Connection Type: {conn.conn_type}")
            print("=" * 60)
        except Exception as e:
            print(f"⚠️ Connection 'clickhouse_conn' not found: {e}")
            print("Please create the connection in Airflow UI > Admin > Connections")

    # ----------------------------------------------------------
    # Task 2: สาธิตการใช้ Hook (concept demo)
    # ----------------------------------------------------------
    @task
    def demo_hook_usage():
        """
        Hook คือ abstraction layer ที่อยู่ระหว่าง Connection กับ Operator

        Flow: Connection (credentials) → Hook (interface) → Operator (action)

        Hook ช่วยให้เราไม่ต้อง:
        - เขียน code เปิด/ปิด connection เอง
        - จัดการ error handling เอง
        - เขียน retry logic เอง
        """
        # ตัวอย่างการใช้ Hook (concept demo)
        # ในงานจริงจะใช้แบบนี้:
        # from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        # hook = S3Hook(aws_conn_id="minio_default")
        # hook.load_file(filename="data.csv", key="raw/data.csv", bucket_name="my-bucket")

        print("=" * 60)
        print("🪝 HOOK CONCEPT DEMO")
        print("=" * 60)
        print()
        print("Hook เป็น interface ที่ช่วยจัดการ connection ให้เรา")
        print("แทนที่จะเขียน boilerplate code เอง Hook จะ:")
        print("  1. เปิด connection ให้อัตโนมัติ")
        print("  2. จัดการ error handling")
        print("  3. ปิด connection ให้เมื่อเสร็จ")
        print()
        print("📝 ตัวอย่าง Hook ที่ใช้บ่อย:")
        print("  - S3Hook         → สำหรับ S3/MinIO (upload, download files)")
        print("  - PostgresHook   → สำหรับ PostgreSQL (run SQL queries)")
        print("  - HttpHook       → สำหรับ REST API (GET, POST requests)")
        print("  - MySqlHook      → สำหรับ MySQL")
        print()
        print("📝 วิธีใช้ Hook ในงานจริง:")
        print('  hook = S3Hook(aws_conn_id="minio_default")')
        print('  hook.load_file("data.csv", "raw/data.csv", "bucket")')
        print("  # Hook จะดึง credentials จาก connection ให้อัตโนมัติ!")

    # ----------------------------------------------------------
    # Task 3: Best practices สำหรับ connections
    # ----------------------------------------------------------
    @task
    def show_connection_best_practices():
        """
        แนวทางที่ดีสำหรับการจัดการ Connections ใน Airflow
        """
        print("=" * 60)
        print("📋 CONNECTION BEST PRACTICES")
        print("=" * 60)
        print()
        print("1. 🔐 อย่า hardcode credentials ใน code เด็ดขาด!")
        print("   ❌ host='localhost', password='secret123'")
        print("   ✅ ใช้ Airflow Connections แทน")
        print()
        print("2. 🏷️ ตั้งชื่อ connection ให้สื่อความหมาย")
        print("   ❌ conn_1, my_conn")
        print("   ✅ minio_default, clickhouse_conn, postgres_dwh")
        print()
        print("3. 🧪 ใช้ test connection ก่อน deploy DAG")
        print("   Airflow UI > Admin > Connections > Test button")
        print()
        print("4. 📦 Export connections เพื่อ version control")
        print("   airflow connections export connections.json")
        print("   airflow connections import connections.json")
        print()
        print("5. 🔄 ใช้ Hook แทน raw connection object")
        print("   Hook จัดการ lifecycle ของ connection ให้")
        print()
        print("6. 🌍 ใช้ Environment Variables สำหรับ production")
        print("   AIRFLOW_CONN_MINIO_DEFAULT='s3://...'")

    # ----------------------------------------------------------
    # Dependencies
    # ----------------------------------------------------------
    # ลำดับ: ดู connections → เรียนรู้ Hook → best practices
    list_available_connections() >> demo_hook_usage() >> show_connection_best_practices()


# สร้าง DAG instance — จำเป็นต้องเรียก function เพื่อให้ Airflow detect DAG
poc_connections_hooks_demo()
