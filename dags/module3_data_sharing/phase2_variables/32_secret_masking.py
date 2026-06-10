from airflow.sdk import dag, task
from airflow.models import Variable
from datetime import datetime


@dag(
    dag_id='32_airflow_secret_masking_demo',
    start_date=datetime(2026, 6, 6),
    schedule=None,
    catchup=False,
    tags=['security', 'learning']
)
def secret_management_dag():


    @task
    def test_logs_masking():
        # 1. ดึงค่าจาก Variable ที่ชื่อเข้าข่ายความลับ (สมมติสร้างไว้บน UI แล้ว)
        # ใส่ Default เผื่อไว้ทดสอบ
        db_pass = Variable.get("database_password", default_var="my_hidden_password_123")
        normal_var = Variable.get("my_api_url", default_var="https://jsonplaceholder.typicode.com")


        # 2. ลองสั่ง print ลง Logs ของ Airflow เพื่อดูผลลัพธ์
        print(f"[LOG] นี่คือค่าของ URL ปกติ: {normal_var}")
        
        # ❌ ฝืนสั่ง print รหัสผ่านตรงๆ
        print(f"[LOG] นี่คือค่าของ รหัสผ่าน: {db_pass}") 
        # ผลลัพธ์ในหน้า UI Logs จะเห็นเป็น: [LOG] นี่คือค่าของ รหัสผ่าน: ***


    test_logs_masking()


secret_management_dag()
