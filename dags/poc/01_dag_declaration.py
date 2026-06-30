"""
01_dag_declaration.py — 3 วิธีในการประกาศ DAG ใน Airflow
==========================================================
📖 Slide Reference: หน้า 30 (Declaring a DAG)

ไฟล์นี้สาธิต 3 รูปแบบการสร้าง DAG:
1. Context Manager  — ใช้ `with DAG() as dag:` (แบบที่นิยมที่สุด)
2. Standard Constructor — สร้าง DAG object แล้วส่งเข้า operator ผ่าน parameter `dag=`
3. @dag Decorator    — TaskFlow API (แบบ Pythonic ที่สุด, แนะนำสำหรับ Airflow 2.x+)

Scenario: pipeline สร้างรายงานยอดขายประจำวัน (Daily Sales Report)
แต่ละ DAG ทำ 3 ขั้นตอนเหมือนกัน: extract → transform → load
แต่ประกาศคนละแบบเพื่อให้เห็นความแตกต่าง
"""

from airflow.sdk import dag, task, DAG
from airflow.operators.python import PythonOperator
import pendulum


# ============================================================================
# วิธีที่ 1: Context Manager — with DAG() as dag
# ============================================================================
# วิธีนี้เป็นที่นิยมมากที่สุดใน Airflow community
# ข้อดี: ไม่ต้องส่ง dag= ให้แต่ละ operator เอง เพราะ context manager จัดการให้
# task ทุกตัวที่อยู่ภายใน `with` block จะถูกผูกกับ DAG นี้โดยอัตโนมัติ

def _extract_sales_cm():
    """ดึงข้อมูลยอดขายจาก source (Context Manager version)"""
    print("📥 [Context Manager] Extracting daily sales data...")
    return {"total_records": 1500, "source": "pos_system"}


def _transform_sales_cm():
    """แปลงข้อมูลยอดขาย — คำนวณสรุป, ทำความสะอาดข้อมูล"""
    print("🔄 [Context Manager] Transforming sales data...")
    print("  - Cleaning nulls, calculating aggregates")


def _load_sales_cm():
    """โหลดข้อมูลเข้า data warehouse"""
    print("📤 [Context Manager] Loading sales report to warehouse...")
    print("  - Inserting into fact_daily_sales table")


with DAG(
    dag_id="poc_declaration_context_manager",
    # schedule=None หมายถึง DAG นี้จะไม่รันอัตโนมัติ ต้อง trigger manually
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    tags=["poc", "declaration"],
    doc_md="""### วิธีที่ 1: Context Manager\nใช้ `with DAG()` เพื่อประกาศ DAG""",
) as dag_context_manager:

    # สร้าง task ด้วย PythonOperator — แต่ละ task อยู่ใน with block
    # จึงถูกผูกกับ dag_context_manager โดยอัตโนมัติ
    extract = PythonOperator(
        task_id="extract_sales",
        python_callable=_extract_sales_cm,
    )

    transform = PythonOperator(
        task_id="transform_sales",
        python_callable=_transform_sales_cm,
    )

    load = PythonOperator(
        task_id="load_sales",
        python_callable=_load_sales_cm,
    )

    # กำหนด dependency ด้วย >> (bitshift operator)
    # อ่านว่า: extract ทำก่อน → แล้วค่อย transform → แล้วค่อย load
    extract >> transform >> load


# ============================================================================
# วิธีที่ 2: Standard Constructor — สร้าง DAG object แยก แล้วส่งเข้า operator
# ============================================================================
# วิธีนี้เป็นแบบดั้งเดิมของ Airflow (legacy style)
# ข้อดี: ชัดเจนว่า task ไหนอยู่ DAG ไหน เพราะต้องระบุ dag= ทุกครั้ง
# ข้อเสีย: ต้องส่ง dag= ให้ทุก operator ทำให้ verbose มากขึ้น
# ⚠️ ถ้าลืมใส่ dag= task นั้นจะไม่ถูกผูกกับ DAG ใดเลย!

dag_constructor = DAG(
    dag_id="poc_declaration_constructor",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    tags=["poc", "declaration"],
    doc_md="""### วิธีที่ 2: Constructor\nสร้าง DAG object แล้วส่งผ่าน `dag=` parameter""",
)


def _extract_sales_cst():
    """ดึงข้อมูลยอดขาย (Constructor version)"""
    print("📥 [Constructor] Extracting daily sales data...")
    return {"total_records": 2000, "source": "e_commerce"}


def _transform_sales_cst():
    """แปลงข้อมูลยอดขาย"""
    print("🔄 [Constructor] Transforming sales data...")


def _load_sales_cst():
    """โหลดข้อมูลเข้า warehouse"""
    print("📤 [Constructor] Loading sales report to warehouse...")


# สังเกต: ต้องส่ง dag=dag_constructor ให้ทุก operator
# ถ้าลืมใส่ task จะ "หลุด" ไม่ปรากฏใน DAG
extract_cst = PythonOperator(
    task_id="extract_sales",
    python_callable=_extract_sales_cst,
    dag=dag_constructor,  # ← ต้องใส่ทุกครั้ง!
)

transform_cst = PythonOperator(
    task_id="transform_sales",
    python_callable=_transform_sales_cst,
    dag=dag_constructor,  # ← ต้องใส่ทุกครั้ง!
)

load_cst = PythonOperator(
    task_id="load_sales",
    python_callable=_load_sales_cst,
    dag=dag_constructor,  # ← ต้องใส่ทุกครั้ง!
)

extract_cst >> transform_cst >> load_cst


# ============================================================================
# วิธีที่ 3: @dag Decorator — TaskFlow API (แนะนำ ✅)
# ============================================================================
# วิธีนี้เป็นแบบ Pythonic ที่สุด ใช้ decorator @dag และ @task
# ข้อดี:
#   - โค้ดสะอาด อ่านง่าย เหมือนเขียน Python function ปกติ
#   - XCom ถูกจัดการอัตโนมัติ (return value = XCom push, parameter = XCom pull)
#   - ไม่ต้องสร้าง PythonOperator เอง
# ข้อเสีย:
#   - ใช้ได้กับ Python task เท่านั้น (BashOperator ต้องใช้แบบอื่น)

@dag(
    dag_id="poc_declaration_decorator",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    tags=["poc", "declaration"],
    doc_md="""### วิธีที่ 3: @dag Decorator\nTaskFlow API — วิธีที่ Pythonic ที่สุด""",
)
def sales_report_decorator():
    """
    DAG สร้างรายงานยอดขายด้วย @dag decorator
    ฟังก์ชันนี้ทำหน้าที่เป็น DAG definition
    """

    @task
    def extract_sales():
        """ดึงข้อมูลยอดขาย — return value จะถูก push เป็น XCom อัตโนมัติ"""
        print("📥 [Decorator] Extracting daily sales data...")
        # return value จะถูกส่งต่อเป็น XCom ให้ task ถัดไปได้เลย
        return {"total_records": 3000, "source": "mobile_app"}

    @task
    def transform_sales(raw_data: dict):
        """
        แปลงข้อมูล — รับ XCom จาก extract โดยอัตโนมัติผ่าน parameter
        ไม่ต้องเรียก xcom_pull() เอง!
        """
        print(f"🔄 [Decorator] Transforming {raw_data['total_records']} records...")
        return {"records_processed": raw_data["total_records"], "status": "cleaned"}

    @task
    def load_sales(transformed_data: dict):
        """โหลดข้อมูลเข้า warehouse"""
        print(f"📤 [Decorator] Loading {transformed_data['records_processed']} records...")
        print("  - Insert complete ✅")

    # กำหนด dependency ผ่าน function call chain
    # extract() → ผลลัพธ์ส่งเข้า transform() → ผลลัพธ์ส่งเข้า load()
    raw = extract_sales()
    transformed = transform_sales(raw)
    load_sales(transformed)


# ⚠️ สำคัญมาก: ต้อง call function เพื่อ instantiate DAG
# ถ้าไม่ call Airflow จะไม่เห็น DAG นี้!
sales_report_decorator()
