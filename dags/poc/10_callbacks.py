"""
=== POC 10: Callbacks (Event Handlers) ===
📖 Slide Reference: หน้า 29 (Callbacks — Event Handlers)

แนวคิดสำคัญ: Callback คืออะไร?

Callback คือ function ที่ Airflow จะเรียกเมื่อเกิด event บางอย่าง
เหมือน "webhook" ที่ fire เมื่อมีเหตุการณ์เกิดขึ้น

ประเภท Callback:
1. DAG-level Callbacks:
   - on_success_callback: เรียกเมื่อ DAG สำเร็จทั้งหมด
   - on_failure_callback: เรียกเมื่อ DAG ล้มเหลว (มี task ที่ fail)

2. Task-level Callbacks:
   - on_success_callback: เรียกเมื่อ task สำเร็จ
   - on_failure_callback: เรียกเมื่อ task ล้มเหลว
   - on_retry_callback: เรียกเมื่อ task กำลังจะ retry

ใช้งานจริง:
- ส่ง notification (Slack, LINE, Email) เมื่อ pipeline สำเร็จ/ล้มเหลว
- บันทึก metric ใน monitoring system
- Trigger pipeline อื่นต่อ
- Clean up resources เมื่อ fail

Scenario: pipeline ที่มี task ล้มเหลวแบบ random เพื่อ demo callback behavior
"""

import random
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


# ============================================================
# Callback Functions — ต้องประกาศ OUTSIDE DAG
# ============================================================
# ⚠️ Callback functions ต้องอยู่นอก DAG definition
# เพราะ Airflow ต้อง serialize และ deserialize ได้


def on_dag_success(context):
    """
    เรียกเมื่อ DAG ทั้งหมดสำเร็จ (ทุก task ผ่านหมด)
    context มีข้อมูลเกี่ยวกับ DAG Run
    """
    dag_id = context["dag"].dag_id
    print(f"🎉 [DAG SUCCESS] DAG '{dag_id}' completed successfully!")
    print(f"   📱 Sending LINE notification...")
    print(f"   📧 Sending Email alert...")
    # ในงานจริง:
    # requests.post("https://notify-api.line.me/api/notify", ...)
    # send_email(to="team@company.com", subject=f"DAG {dag_id} Success", ...)


def on_dag_failure(context):
    """
    เรียกเมื่อ DAG ล้มเหลว (มี task ที่ fail และไม่ retry แล้ว)
    context มีข้อมูลเกี่ยวกับ task ที่ fail
    """
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    print(f"🚨 [DAG FAILURE] DAG '{dag_id}' failed at task '{task_id}'!")
    print(f"   📱 Sending urgent LINE notification...")
    print(f"   📧 Sending failure Email...")
    # ในงานจริง: ส่ง alert แบบ urgent priority


def on_task_success(context):
    """
    เรียกเมื่อ task สำเร็จ
    ใช้สำหรับ log หรือ track progress
    """
    task_id = context["task_instance"].task_id
    print(f"✅ [TASK SUCCESS] Task '{task_id}' completed!")


def on_task_failure(context):
    """
    เรียกเมื่อ task ล้มเหลว (ก่อน retry ถ้ามี)
    ใช้สำหรับ alert ทีม ส่ง notification
    """
    task_id = context["task_instance"].task_id
    exception = context.get("exception", "Unknown")
    print(f"❌ [TASK FAILURE] Task '{task_id}' failed!")
    print(f"   Error: {exception}")
    print(f"   💡 In production, send Slack/LINE alert here")


def on_task_retry(context):
    """
    เรียกเมื่อ task กำลังจะ retry
    ใช้สำหรับ log retry attempt
    """
    task_id = context["task_instance"].task_id
    try_number = context["task_instance"].try_number
    print(f"🔄 [TASK RETRY] Task '{task_id}' retrying (attempt #{try_number})")


# ============================================================
# DAG Definition
# ============================================================
@dag(
    dag_id="poc_callbacks_alerting",
    schedule=None,  # Manual trigger เท่านั้น
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    # ⭐ DAG-level callbacks: fire เมื่อ DAG สำเร็จ/ล้มเหลว
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
    tags=["poc", "callbacks"],
    doc_md="""
    ## POC: Callbacks & Alerting
    - สาธิต DAG-level และ Task-level callbacks
    - Task 2 จะ random fail เพื่อ demo failure callback
    - ลอง run หลายๆ ครั้งเพื่อดู behavior ทั้ง success และ failure
    """,
)
def poc_callbacks_alerting():

    # ----------------------------------------------------------
    # Task 1: เตรียมข้อมูล (จะสำเร็จเสมอ)
    # ----------------------------------------------------------
    @task(
        # ⭐ Task-level callback: fire เมื่อ task นี้สำเร็จ
        on_success_callback=on_task_success,
    )
    def step_prepare_data():
        """
        Task ธรรมดาที่สำเร็จเสมอ
        มี on_success_callback เพื่อ track ว่า task เสร็จแล้ว
        """
        print("📋 Preparing data...")
        print("   Loading configuration...")
        print("   Validating input parameters...")
        print("   ✅ Data preparation complete!")
        return {"status": "ready", "records": 1000}

    # ----------------------------------------------------------
    # Task 2: Process ข้อมูล (RANDOM FAIL เพื่อ demo callback)
    # ----------------------------------------------------------
    @task(
        # ⭐ Task-level callbacks สำหรับ failure และ retry
        on_failure_callback=on_task_failure,
        on_retry_callback=on_task_retry,
        # retry settings: ลอง 2 ครั้ง ห่างกัน 5 วินาที
        retries=2,
        retry_delay=timedelta(seconds=5),
    )
    def step_process_data():
        """
        Task ที่ random fail ~50% ของเวลา
        เพื่อ demo:
        - on_failure_callback: เรียกเมื่อ fail
        - on_retry_callback: เรียกเมื่อ retry
        - ถ้า retry สำเร็จ → DAG success
        - ถ้า retry หมด → DAG failure

        💡 ลอง run หลายๆ ครั้งเพื่อดู behavior ทั้ง 2 แบบ
        """
        print("⚙️ Processing data...")

        # Random fail 50% ของเวลา
        if random.random() < 0.5:
            print("   💥 Something went wrong!")
            raise ValueError("Simulated failure! Random error occurred during processing.")

        print("   ✅ Processing complete!")
        return {"status": "processed", "output_records": 950}

    # ----------------------------------------------------------
    # Task 3: บันทึกผลลัพธ์ (จะสำเร็จเสมอ ถ้า task 2 ผ่าน)
    # ----------------------------------------------------------
    @task(
        on_success_callback=on_task_success,
    )
    def step_save_results():
        """
        Task สุดท้าย — บันทึกผลลัพธ์
        จะ run ได้ก็ต่อเมื่อ step_process_data สำเร็จ
        """
        print("💾 Saving results...")
        print("   Writing to database...")
        print("   Generating report...")
        print("   ✅ Results saved successfully!")

    # ----------------------------------------------------------
    # Dependencies: prepare >> process >> save
    # ----------------------------------------------------------
    step_prepare_data() >> step_process_data() >> step_save_results()


# สร้าง DAG instance — จำเป็นต้องเรียก function เพื่อให้ Airflow detect DAG
poc_callbacks_alerting()
