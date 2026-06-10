"""
22_dag_params.py — Passing Parameters when triggering DAGs
Module 2, Phase 4: Organization & Dynamic

ส่ง JSON configuration เมื่อ Trigger DAG ผ่าน UI/CLI/API
"""
from airflow.sdk import dag, task
from datetime import datetime


# กำหนด Default Params ไว้เผื่อกรณีที่ไม่ได้ใส่ JSON Config ตอน Trigger
DEFAULT_CONFIG = {
    "target_table": "users_table",
    "batch_size": 100
}


@dag(
    dag_id="22_dag_params",
    start_date=datetime(2026, 6, 6),
    schedule=None,
    catchup=False,
    params=DEFAULT_CONFIG,  # ใช้ params ในการตั้งค่าเริ่มต้นที่หน้า UI
    tags=["params"],
)
def dag_run_config_demo():

    @task
    def process_data(**context):
        # 1. วิธีดึงค่าจาก JSON ที่ส่งเข้ามา (ผ่าน dag_run.conf)
        dag_run_conf = context.get('dag_run').conf

        # 2. ผสมผสานค่าจาก dag_run.conf (ถ้ามี) หรือใช้ค่าจาก params (ถ้าไม่มี)
        target = dag_run_conf.get('target_table', context['params']['target_table'])
        batch = dag_run_conf.get('batch_size', context['params']['batch_size'])

        print(f"🚀 กำลังประมวลผลไปที่ตาราง: {target}")
        print(f"📦 จำนวน Batch Size คือ: {batch}")

        return f"Processed {target} with {batch} records"

    process_data()


dag_run_config_demo()
