from airflow.sdk import  dag, task
from datetime import datetime
import clickhouse_driver  # ❌ ต้องนำเข้า Driver ของ ClickHouse มาจัดการเปิด-ปิดเองภายนอก


@dag(
    dag_id='33_bad_practice_clickhouse_v1', 
    start_date=datetime(2026, 6, 6), 
    schedule=None, 
    catchup=False,
    tags=['bad_practice', 'clickhouse']
)
def bad_clickhouse_dag():


    @task
    def extract_clickhouse_bad_way():
        # ❌ ปัญหาที่ 1: Hardcode รหัสผ่านของ Data Warehouse ไว้ในไฟล์ Python ตรงๆ
        # (ความลับของบริษัทหลุดทันทีถ้าไฟล์นี้หลุดไปใน Git และถ้าเปลี่ยนรหัสผ่านที ต้องแก้กันสนุกสนาน)
        CH_HOST = "clickhouse-prod.company.com"
        CH_PORT = 9000  # Native port ของ ClickHouse
        CH_USER = "analytics_user"
        CH_PASSWORD = "ClickHouseSecretPassword2026!"  # รหัสผ่าน Warehouse หลุดเต็มๆ!
        CH_DATABASE = "data_warehouse"


        print(f"[INFO] กำลังเชื่อมต่อไปยัง ClickHouse Cluster: {CH_HOST}...")


        # ❌ ปัญหาที่ 2: ต้องเขียนโค้ดจัดการการเชื่อมต่อเอง (ถ้าเขียนพลาดเสี่ยงทำ Connection Leak ระบบล่ม)
        try:
            # สร้าง Connection Object ขึ้นมาตรงๆ
            client = clickhouse_driver.Client(
                host=CH_HOST,
                port=CH_PORT,
                user=CH_USER,
                password=CH_PASSWORD,
                database=CH_DATABASE
            )
            
            # สั่งยิงคิวรีสไตล์ ClickHouse 
            sql_query = "SELECT event_id, event_name, event_date FROM events_log LIMIT 5;"
            results = client.execute(sql_query)
            
            # ❌ ปัญหาที่ 3: ความลับสุ่มเสี่ยงหลุดไปที่หน้า UI Logs
            print(f"[SUCCESS] ดึงข้อมูลสำเร็จด้วยสิทธิ์ {CH_USER} (รหัสผ่านที่ใช้: {CH_PASSWORD})")
            
            return results


        except Exception as error:
            print(f"[ERROR] ระบบเชื่อมต่อ Clickhouse พังพินาศ: {error}")
            raise error
            
        # หมายเหตุ: clickhouse-driver ตัว Client จะจัดการตัดการเชื่อมต่อตอนจบฟังก์ชันให้ 
        # แต่ในแง่โครงสร้างโค้ด เรายังคงต้องแบกรับความเสี่ยงเรื่องรหัสผ่านหลุดอยู่ดี


    extract_clickhouse_bad_way()


bad_clickhouse_dag()
