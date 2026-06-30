"""
02_operators.py — Operator พื้นฐานใน Airflow
==============================================
📖 Slide Reference: หน้า 31 (Operators)

ไฟล์นี้สาธิต Operator 3 แบบที่ใช้บ่อยที่สุด:
1. BashOperator   — รัน shell command (เช่น bash script, CLI tools)
2. PythonOperator — รัน Python function (แบบดั้งเดิม)
3. @task decorator — รัน Python function แบบ TaskFlow API (แนะนำ)

Scenario: System Health Check Pipeline
- ตรวจสอบว่า service ยังทำงานอยู่ (BashOperator)
- ตรวจสอบ disk usage (PythonOperator)
- รวมผลลัพธ์และแสดง summary (@task)

💡 เปรียบเทียบ:
- BashOperator: เหมาะกับ shell commands, scripts ที่มีอยู่แล้ว
- PythonOperator: เหมาะกับ Python logic ที่ซับซ้อน (legacy style)
- @task: เหมาะกับ Python logic ทั่วไป (modern style, XCom อัตโนมัติ)
"""

import shutil

from airflow.sdk import dag, task, DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum


@dag(
    dag_id="poc_operators_health_check",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    tags=["poc", "operators"],
    doc_md="""
    ### System Health Check Pipeline
    สาธิตการใช้ BashOperator, PythonOperator, และ @task decorator
    """,
)
def health_check_pipeline():
    """
    Pipeline ตรวจสุขภาพระบบ
    รวม 3 operator types ไว้ใน DAG เดียว
    """

    # ====================================================================
    # Task 1: BashOperator — รัน shell command
    # ====================================================================
    # BashOperator เหมาะสำหรับ:
    #   - รัน shell script ที่มีอยู่แล้ว
    #   - เรียก CLI tools (curl, aws, gcloud, etc.)
    #   - ทำ system-level operations
    #
    # ⚠️ bash_command ต้องลงท้ายด้วย space ถ้าเป็น command เดียว
    #    (เพื่อป้องกัน Airflow ตีความเป็น script file path)
    #
    # 💡 ค่าที่ echo ออกมาบรรทัดสุดท้ายจะถูก push เป็น XCom อัตโนมัติ
    #    (ถ้าไม่ต้องการให้ set do_xcom_push=False)
    bash_check = BashOperator(
        task_id="check_service_alive",
        bash_command='echo "Service is alive" && date ',
        # do_xcom_push=True (default) — ผลลัพธ์บรรทัดสุดท้ายจะถูก push เป็น XCom
    )

    # ====================================================================
    # Task 2: PythonOperator — รัน Python function
    # ====================================================================
    # PythonOperator เป็นแบบดั้งเดิม (legacy) สำหรับรัน Python code
    # ข้อดี: ควบคุมได้ละเอียด เช่น op_args, op_kwargs, templates_dict
    # ข้อเสีย: ต้อง xcom_push/xcom_pull เอง ไม่สะดวกเท่า @task
    #
    # 💡 ถ้า python_callable return ค่า จะถูก push เป็น XCom อัตโนมัติ
    #    (key="return_value")

    def _check_disk_usage(**kwargs):
        """
        ตรวจสอบ disk usage ของ root filesystem
        ใช้ shutil.disk_usage() ซึ่งเป็น standard library

        **kwargs จะมี context ของ Airflow เช่น ti, ds, execution_date
        """
        usage = shutil.disk_usage("/")

        total_gb = usage.total / (1024 ** 3)
        used_gb = usage.used / (1024 ** 3)
        free_gb = usage.free / (1024 ** 3)
        used_pct = (usage.used / usage.total) * 100

        result = {
            "total_gb": round(total_gb, 2),
            "used_gb": round(used_gb, 2),
            "free_gb": round(free_gb, 2),
            "used_percent": round(used_pct, 2),
        }

        print(f"💾 Disk Usage Report:")
        print(f"  Total: {result['total_gb']} GB")
        print(f"  Used:  {result['used_gb']} GB ({result['used_percent']}%)")
        print(f"  Free:  {result['free_gb']} GB")

        # ⚠️ ถ้า used > 90% ควร alert (ตัวอย่างเฉยๆ ไม่ได้ทำจริง)
        if used_pct > 90:
            print("🚨 WARNING: Disk usage exceeds 90%!")

        # return value จะถูก push เป็น XCom ให้ task อื่นดึงไปใช้ได้
        return result

    python_check = PythonOperator(
        task_id="check_disk_usage",
        python_callable=_check_disk_usage,
        # op_kwargs={"path": "/"} — ตัวอย่างการส่ง keyword arguments
    )

    # ====================================================================
    # Task 3: @task decorator — TaskFlow API
    # ====================================================================
    # @task เป็นวิธีที่แนะนำ (modern style)
    # ข้อดี:
    #   - XCom ถูกจัดการอัตโนมัติ (return = push, parameter = pull)
    #   - โค้ดสะอาด เหมือนเขียน Python ปกติ
    #   - ไม่ต้องสร้าง PythonOperator เอง
    #
    # 💡 สังเกต: parameter ของ function = XCom value จาก task ก่อนหน้า

    @task
    def generate_health_summary(service_status: str, disk_info: dict):
        """
        รวมผลลัพธ์จาก bash_check และ python_check แล้วแสดง summary

        Parameters:
            service_status: ผลจาก BashOperator (XCom — last line of stdout)
            disk_info: ผลจาก PythonOperator (XCom — return value dict)
        """
        print("=" * 60)
        print("📊 SYSTEM HEALTH CHECK SUMMARY")
        print("=" * 60)
        print(f"🟢 Service Status : {service_status}")
        print(f"💾 Disk Total     : {disk_info['total_gb']} GB")
        print(f"💾 Disk Used      : {disk_info['used_gb']} GB ({disk_info['used_percent']}%)")
        print(f"💾 Disk Free      : {disk_info['free_gb']} GB")

        # ประเมินสถานะโดยรวม
        if disk_info["used_percent"] < 80:
            overall = "HEALTHY ✅"
        elif disk_info["used_percent"] < 90:
            overall = "WARNING ⚠️"
        else:
            overall = "CRITICAL 🚨"

        print(f"\n🏥 Overall Status : {overall}")
        print("=" * 60)

    # ====================================================================
    # Dependencies — กำหนดลำดับการทำงาน
    # ====================================================================
    # [bash_check, python_check] >> summary
    # หมายถึง: bash_check และ python_check รันพร้อมกัน (parallel)
    #          เสร็จทั้งคู่แล้วค่อยรัน summary
    #
    # สังเกต: เราส่ง output ของ bash_check และ python_check
    #         เข้าเป็น parameter ของ generate_health_summary()
    #         ซึ่ง TaskFlow API จะจัดการ XCom + dependency ให้อัตโนมัติ

    # เมื่อส่ง output เป็น argument ให้ @task function
    # Airflow จะสร้าง dependency อัตโนมัติ (ไม่ต้องใช้ >>)
    service_result = bash_check.output  # XCom output จาก BashOperator
    disk_result = python_check.output   # XCom output จาก PythonOperator

    generate_health_summary(service_result, disk_result)


# ⚠️ สำคัญ: ต้อง call function เพื่อ instantiate DAG
health_check_pipeline()
