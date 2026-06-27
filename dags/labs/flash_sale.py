"""
Lab 1 — Flash Sale Pipeline (BROKEN VERSION)
=============================================
โจทย์: มีแฟลชเซล 11.11 ลูกค้าสั่งซื้อพร้อมกัน
       API ล่มกลางทาง ระบบ retry อัตโนมัติ
       ถ้า pipeline ไม่ idempotent → ลูกค้าถูกตัดเงิน 2 ครั้ง!

ภารกิจ:
  1. รัน DAG นี้แล้วดูว่ามีออเดอร์ซ้ำใน DB กี่แถว
  2. แก้ task `insert_orders` ให้ idempotent
  3. Clear และ re-run 3 ครั้ง ตรวจสอบว่า count ไม่เปลี่ยน
  4. (Bonus) จำลอง timeout ด้วย time.sleep() แล้ว retry

hint: ดูที่ TODO ในโค้ด
"""

from airflow.sdk import dag, task
from datetime import datetime, timedelta
import clickhouse_connect
import random

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


def get_client():
    # Inside Docker Compose, host is 'clickhouse' and password is 'clickhouse'
    client = clickhouse_connect.get_client(host='clickhouse', port=8123, username='default', password='clickhouse')
    client.query("CREATE DATABASE IF NOT EXISTS labs")
    client.query("""
        CREATE TABLE IF NOT EXISTS labs.orders (
            order_id    String,
            customer_id String,
            product     String,
            amount      Float64,
            sale_date   String,
            created_at  DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY order_id
    """)
    return client


def generate_mock_orders(sale_date: str) -> list:
    """สร้าง mock orders สำหรับวันนั้น (seed เดิม = ข้อมูลเดิมทุกครั้ง)"""
    random.seed(sale_date)
    products = ["iPhone 16", "AirPods Pro", "MacBook Air", "iPad Mini", "Apple Watch"]
    orders = []
    for i in range(10):
        orders.append({
            "order_id":    f"ORD-{sale_date}-{i+1:03d}",
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "product":     random.choice(products),
            "amount":      round(random.uniform(1000, 80000), 2),
            "sale_date":   sale_date,
        })
    return orders


@dag(
    dag_id="lab1_flash_sale",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime.now() - timedelta(days=3),
    catchup=False,
    tags=["lab1", "idempotency"],
    doc_md=__doc__,
)
def lab1_flash_sale():

    @task
    def extract(ds=None):
        """ดึง orders จาก mock API"""
        print(f"[extract] กำลังดึง orders วันที่ {ds}")
        orders = generate_mock_orders(ds)
        print(f"[extract] ได้ {len(orders)} orders")
        return orders

    @task
    def insert_orders(orders: list, ds=None):
        """
        TODO: แก้ function นี้ให้ idempotent!

        ปัญหาปัจจุบัน: ใช้ INSERT ธรรมดา
        → รันซ้ำ = ข้อมูลซ้ำทุกครั้ง

        วิธีแก้ (เลือกอย่างใดอย่างหนึ่ง):
          A) DELETE วันนั้นก่อน แล้วค่อย INSERT ใหม่ใน transaction เดียว
          B) ใช้ INSERT OR REPLACE INTO ... (SQLite UPSERT)
        """
        client = get_client()

        # ❌ BUG: INSERT ธรรมดา — รันซ้ำ = ข้อมูลซ้ำ!

        rows = [
            (order["order_id"], order["customer_id"], order["product"], order["amount"], order["sale_date"])
            for order in orders
        ]
        
        client.insert(
            "labs.orders",
            rows,
            column_names=["order_id", "customer_id", "product", "amount", "sale_date"]
        )

        # ตรวจสอบ
        result = client.query("SELECT count() FROM labs.orders WHERE sale_date = {ds:String}", parameters={"ds": ds})
        total = result.result_rows[0][0]
        print(f"[insert_orders] rows ใน DB วัน {ds}: {total}")
        print(f"[insert_orders] คาดหวัง 10, จริง {total} {'✅' if total == 10 else '❌ มีซ้ำ!'}")

    @task
    def summarize(ds=None):
        """สรุปยอดขายรายวัน"""
        client = get_client()
        result = client.query(
            "SELECT count() as cnt, sum(amount) as rev FROM labs.orders WHERE sale_date = {ds:String}",
            parameters={"ds": ds}
        )
        row = result.result_rows[0]
        count = row[0]
        revenue = row[1] if row[1] is not None else 0.0

        print(f"\n{'='*40}")
        print(f"Flash Sale Summary — {ds}")
        print(f"  Orders  : {count}")
        print(f"  Revenue : {revenue:,.2f} THB")
        print(f"{'='*40}\n")
        if count != 10:
            raise ValueError(f"❌ พบ {count} orders แต่คาดหวัง 10 — มีข้อมูลซ้ำ!")

    orders_data = extract()
    insert_orders(orders_data) >> summarize()


lab1_flash_sale()