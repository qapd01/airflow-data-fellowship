from airflow.sdk import  dag, task
from datetime import datetime
# ✅ นำเข้า Hook จาก Plugin ที่ถูกต้องตามที่คุณตรวจเจอ
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


@dag(
    dag_id='35_good_practice_clickhouse_fixed', 
    start_date=datetime(2026, 6, 6), 
    schedule=None, 
    catchup=False,
    tags=['good_practice', 'clickhouse']
)
def good_clickhouse_dag():


    @task
    def extract_and_query_clickhouse():
        # ⭐️ ดึงค่าสิทธิ์จาก Connection ID บนหน้า UI ผ่าน ClickHouseHook ตัวจริง
        ch_hook = ClickHouseHook(clickhouse_conn_id='my_clickhouse_prod')
        
        print("[INFO] เชื่อมต่อ ClickHouse ผ่าน Connection ที่ตั้งไว้บน UI สำเร็จ")


        try:
            # คำสั่งคิวรีทดสอบ
            sql_query = "SELECT event_id, event_name FROM events_log LIMIT 5;"
            
            # ⭐️ ข้อควรระวัง: ClickHouseHook ของ Plugin นี้จะใช้เมธอดชื่อ .execute() 
            # ในการรันคำสั่ง (ต่างจากฐานข้อมูลทั่วไปที่มักใช้ .get_records())
            results = ch_hook.execute(sql_query)
            
            print(f"[SUCCESS] ดึงข้อมูลสำเร็จ! จำนวนที่ได้: {len(results)} แถว")
            return results


        except Exception as error:
            print(f"[ERROR] เกิดข้อผิดพลาดในการคิวรีข้อมูล: {error}")
            raise error


    extract_and_query_clickhouse()


good_clickhouse_dag()
