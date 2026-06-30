"""
03_task_dependencies.py — การกำหนด Task Dependencies ใน Airflow
================================================================
📖 Slide Reference: หน้า 32 (Task Dependencies)

ไฟล์นี้สาธิตวิธีกำหนดลำดับการทำงานของ task (dependencies):
1. >> (bitshift right)    — task_a >> task_b  หมายถึง "a ทำก่อน b"
2. << (bitshift left)     — task_b << task_a  หมายถึง "b ขึ้นอยู่กับ a" (เหมือนข้อ 1)
3. set_downstream()       — task_a.set_downstream(task_b) เหมือน >>
4. set_upstream()         — task_b.set_upstream(task_a) เหมือน <<

💡 Fan-out / Fan-in Pattern:
                      ┌─ validate_payment ─┐
   extract_order ─────┤                    ├──── generate_report ── notify_customer
                      └─ validate_stock  ──┘

Fan-out: 1 task แตกออกเป็นหลาย task (extract → [payment, stock])
Fan-in:  หลาย task รวมกลับเป็น 1 task ([payment, stock] → report)

Scenario: E-Commerce Order Processing Pipeline
"""

from airflow.sdk import dag, task
import pendulum


@dag(
    dag_id="poc_dependencies_ecommerce",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    tags=["poc", "dependencies"],
    doc_md="""
    ### E-Commerce Order Processing
    สาธิต Task Dependencies: `>>`, `<<`, `set_upstream`, `set_downstream`
    พร้อม Fan-out / Fan-in pattern
    """,
)
def ecommerce_order_processing():
    """
    Pipeline ประมวลผลคำสั่งซื้อ e-commerce
    สาธิตการกำหนด dependency หลายรูปแบบ
    """

    # ====================================================================
    # Task Definitions
    # ====================================================================

    @task
    def extract_order():
        """
        ดึงข้อมูลคำสั่งซื้อจากระบบ
        นี่คือ task แรก — ไม่มี upstream dependency
        """
        order = {
            "order_id": "ORD-2026-001",
            "customer": "สมชาย",
            "items": ["laptop", "mouse"],
            "total": 45000.00,
            "payment_method": "credit_card",
        }
        print(f"📦 Extracted order: {order['order_id']}")
        print(f"   Customer: {order['customer']}")
        print(f"   Total: ฿{order['total']:,.2f}")
        return order

    @task
    def validate_payment(order: dict):
        """
        ตรวจสอบการชำระเงิน
        Fan-out: task นี้รันพร้อมกับ validate_stock
        """
        print(f"💳 Validating payment for order {order['order_id']}...")
        print(f"   Method: {order['payment_method']}")
        print(f"   Amount: ฿{order['total']:,.2f}")
        # simulate validation
        result = {"order_id": order["order_id"], "payment_valid": True}
        print(f"   ✅ Payment validated!")
        return result

    @task
    def validate_stock(order: dict):
        """
        ตรวจสอบสต็อกสินค้า
        Fan-out: task นี้รันพร้อมกับ validate_payment
        """
        print(f"📦 Checking stock for order {order['order_id']}...")
        for item in order["items"]:
            print(f"   Checking '{item}' → In stock ✅")
        result = {"order_id": order["order_id"], "stock_valid": True}
        return result

    @task
    def generate_report(payment_result: dict, stock_result: dict):
        """
        สร้างรายงานสรุปคำสั่งซื้อ
        Fan-in: รอทั้ง validate_payment และ validate_stock เสร็จก่อน
        """
        print(f"📊 Generating report for order {payment_result['order_id']}...")
        report = {
            "order_id": payment_result["order_id"],
            "payment_ok": payment_result["payment_valid"],
            "stock_ok": stock_result["stock_valid"],
            "status": "approved",
        }
        print(f"   Payment: {'✅' if report['payment_ok'] else '❌'}")
        print(f"   Stock:   {'✅' if report['stock_ok'] else '❌'}")
        print(f"   Status:  {report['status'].upper()}")
        return report

    @task
    def notify_customer(report: dict):
        """
        แจ้งเตือนลูกค้า — task สุดท้ายใน pipeline
        """
        print(f"📧 Notifying customer about order {report['order_id']}...")
        print(f"   Status: {report['status']}")
        print(f"   Message: คำสั่งซื้อ {report['order_id']} ได้รับการอนุมัติแล้ว ✅")

    # ====================================================================
    # วิธีที่ 1: >> (Bitshift Right) — วิธีที่นิยมที่สุด ✅
    # ====================================================================
    # อ่านจากซ้ายไปขวา: "extract ทำก่อน แล้วค่อย [payment, stock]"
    #
    # สำหรับ TaskFlow API (@task) การส่ง output เป็น argument
    # จะสร้าง dependency อัตโนมัติ ไม่ต้องใช้ >> เพิ่ม
    # แต่เราสามารถใช้ >> เพิ่มได้ถ้าต้องการ

    order_data = extract_order()

    # Fan-out: extract → [validate_payment, validate_stock]
    # เมื่อส่ง order_data เป็น parameter → dependency ถูกสร้างอัตโนมัติ
    payment = validate_payment(order_data)
    stock = validate_stock(order_data)

    # Fan-in: [validate_payment, validate_stock] → generate_report
    report = generate_report(payment, stock)

    # ====================================================================
    # วิธีที่ 2: set_downstream() — เหมือน >> แต่เป็น method call
    # ====================================================================
    # report.set_downstream(notification) เหมือนกับ report >> notification
    # ข้อดี: อ่านชัดเจนกว่าสำหรับคนที่ไม่คุ้น operator overloading
    #
    # ตัวอย่าง (ใช้ได้ทั้งสองแบบ — เลือกแบบใดแบบหนึ่ง):
    #   report >> notify_customer(report)           # แบบ >>
    #   report.set_downstream(notify_customer(report))  # แบบ method

    notification = notify_customer(report)

    # ====================================================================
    # วิธีที่ 3: << (Bitshift Left / Reverse) — อ่านกลับด้าน
    # ====================================================================
    # task_b << task_a หมายถึง "b depends on a" หรือ "a ทำก่อน b"
    # เหมือน task_a >> task_b แต่เขียนกลับด้าน
    #
    # ⚠️ ใช้ << ได้ แต่ไม่แนะนำเพราะอ่านสับสน
    # ตัวอย่าง (ไม่ได้ใช้จริงในไฟล์นี้ เพราะ dependency ถูกสร้างจาก
    #           function call chain แล้ว แต่แสดงให้เห็น syntax):
    #
    #   notification << report   # notification ขึ้นอยู่กับ report
    #   เหมือนกับ: report >> notification
    #
    # สำหรับ TaskFlow API ที่ส่ง output เป็น argument
    # dependency จะถูกสร้างจาก function call อยู่แล้ว
    # การใช้ << ซ้ำจะ redundant แต่ไม่ error

    # ====================================================================
    # วิธีที่ 4: set_upstream() — เหมือน << แต่เป็น method call
    # ====================================================================
    # task_b.set_upstream(task_a) หมายถึง "b ขึ้นอยู่กับ a"
    # เหมือน task_a >> task_b หรือ task_b << task_a
    #
    # ตัวอย่าง (ไม่ได้ใช้จริง เพราะ dependency สร้างแล้ว):
    #   notification.set_upstream(report)  # notification ขึ้นอยู่กับ report

    # ====================================================================
    # สรุปวิธีกำหนด dependency ทั้ง 4 แบบ:
    # ====================================================================
    #
    # | วิธี               | ตัวอย่าง                        | ความหมาย           |
    # |--------------------|---------------------------------|-------------------|
    # | >> (bitshift)      | task_a >> task_b                | a ทำก่อน b         |
    # | << (reverse)       | task_b << task_a                | b ขึ้นอยู่กับ a     |
    # | set_downstream()   | task_a.set_downstream(task_b)   | a → b             |
    # | set_upstream()     | task_b.set_upstream(task_a)      | b ← a             |
    # | function call      | task_b(task_a())                | a → b (TaskFlow)  |
    #
    # 💡 แนะนำ: ใช้ >> เป็นหลัก เพราะอ่านง่ายและเป็นที่นิยมที่สุด
    # 💡 TaskFlow API: ส่ง output เป็น parameter จะสร้าง dependency อัตโนมัติ


# ⚠️ สำคัญ: ต้อง call function เพื่อ instantiate DAG
ecommerce_order_processing()
