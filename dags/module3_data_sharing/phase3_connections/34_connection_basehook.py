from airflow.sdk import  dag, task
from datetime import datetime
# ✅ นำเข้า BaseHook เพื่อใช้สำหรับไปดึงก้อนข้อมูลจากตู้เซฟ Connection หลังบ้าน
from airflow.hooks.base import BaseHook
import clickhouse_driver


@dag(
    dag_id='34_print_and_use_clickhouse_connection', 
    start_date=datetime(2026, 6, 6), 
    schedule=None, 
    catchup=False,
    tags=['learning', 'clickhouse']
)
def clickhouse_conn_dag():


    @task
    def extract_clickhouse_with_conn_details():
        # 1. ระบุชื่อ Conn ID ที่คุณไปตั้งค่าไว้บนหน้า Airflow UI
        CONN_ID = 'my_clickhouse_prod'
        
        # 2. ดึงออบเจกต์ Connection ออกมาจากระบบหลังบ้าน
        conn = BaseHook.get_connection(CONN_ID)
        
        # 3. แกะฟิลด์ข้อมูลออกมาเก็บไว้ในตัวแปร
        CH_HOST = conn.host
        CH_PORT = conn.port or 9000  # ถ้าใน UI ไม่ได้กรอก ให้ใส่ 9000 เป็น default
        CH_USER = conn.login
        CH_DATABASE = conn.schema     # ฟิลด์ Schema บน UI จะถูกเก็บในตัวแปร .schema
        
        # ⚠️ จุดสำคัญ: การแกะรหัสผ่านที่ถูกถอดรหัสออกมาเป็น Plain Text ต้องใช้ฟังก์ชัน .get_password()
        CH_PASSWORD = conn.get_password()


        # -----------------------------------------------------------
        # 🎯 ส่วนที่ 1: สั่งพิมพ์ (Print) ข้อมูลทั้งหมดเพื่อใช้ตรวจดู
        # -----------------------------------------------------------
        print(f"===== [DEBUG] ข้อมูลที่แกะมาจาก Conn ID: {CONN_ID} =====")
        print(f"🔹 Host: {CH_HOST}")
        print(f"🔹 Port: {CH_PORT}")
        print(f"🔹 User: {CH_USER}")
        print(f"🔹 Database: {CH_DATABASE}")
        
        # 🛡️ ไฮไลต์สำหรับสอนนักเรียน (Secrets Masking):
        # แม้คุณจะใช้คำสั่ง print() เพื่อพ่นรหัสผ่านตัวจริงออกมาตรงๆ 
        # ทันทีที่ระบบ Airflow แอบเห็นว่าข้อความนี้ถูกแกะมาจากฟังก์ชันรหัสผ่าน 
        # ระบบ Logs บนหน้า UI ของ Airflow จะคาดดำเปลี่ยนข้อความนี้เป็น *** ให้เราโดยอัตโนมัติ!
        print(f"🔒 Password (Plain Text): {CH_PASSWORD}")
        print("======================================================")


        # -----------------------------------------------------------
        # ⚡ ส่วนที่ 2: นำตัวแปรที่ดึงมาได้ไปเสียบต่อกับ Driver จริง
        # -----------------------------------------------------------
        try:
            client = clickhouse_driver.Client(
                host=CH_HOST,
                port=int(CH_PORT),
                user=CH_USER,
                password=CH_PASSWORD,
                database=CH_DATABASE
            )
            
            sql_query = "SELECT event_id, event_name, event_date FROM events_log LIMIT 5;"
            results = client.execute(sql_query)
            
            print(f"[SUCCESS] ดึงข้อมูลจาก ClickHouse สำเร็จ! จำนวนที่ได้: {len(results)} แถว")
            return results


        except Exception as error:
            print(f"[ERROR] ระบบเชื่อมต่อ Clickhouse พังพินาศ: {error}")
            raise error


    extract_clickhouse_with_conn_details()


clickhouse_conn_dag()
