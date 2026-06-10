"""
20_dag_documentation.py — DAG & Task Documentation (doc_md)
Module 2, Phase 4: Organization & Dynamic

แสดง Rich Content (Markdown) ใน Web Interface ของ Airflow
รวม Setup/Teardown, TaskGroup, Edge Labels, Branch
"""
from airflow.sdk import dag, task, task_group, setup, teardown
from airflow.utils.edgemodifier import Label
from datetime import datetime


# Documentation in Markdown
VBH_DOC = """
# 📊 [vbh] Daily ETL Pipeline
**Project**: Data Engineering System
**Version**: 1.0.0
**Description**:
ระบบประมวลผลข้อมูลอัตโนมัติประกอบด้วย:
- **Setup**: เตรียม Resource สำหรับการรันงาน
- **Processing**: ทำการกรองและจัดกลุ่มข้อมูล
- **Teardown**: ล้างข้อมูลชั่วคราวเพื่อประหยัด Cost

> **Note**: ข้อมูลจะถูกเก็บใน ClickHouse ทุกขั้นตอนรันแบบ Atomic
"""


@dag(
    dag_id="20_dag_documentation",
    start_date=datetime(2026, 6, 6),
    schedule="@daily",
    catchup=False,
    doc_md=VBH_DOC,
    default_args={"owner": "Sorawid"}
)
def vbh_pipeline():

    @setup
    def setup_system():
        print("[vbh] Setup: Resource initialized.")
        return "ready"

    @task_group(group_id="data_processing_group")
    def process_group(status):
        @task
        def clean():
            print(f"[vbh] Cleaning data with status: {status}")
            return True

        @task.branch
        def check_quality(is_clean):
            return "data_processing_group.high_quality" if is_clean else "data_processing_group.low_quality"

        @task
        def high_quality():
            print("[vbh] Quality: High")

        @task
        def low_quality():
            print("[vbh] Quality: Low")

        clean_status = clean()
        clean_status >> check_quality(clean_status) >> [high_quality(), low_quality()]

    @teardown
    def teardown_system():
        print("[vbh] Teardown: Resource cleaned.")

    # Execution Flow
    s = setup_system()
    t = teardown_system()

    # Define dependencies with labels
    s >> Label("Start Process") >> process_group(s) >> Label("End Process") >> t


# Trigger the DAG
vbh_pipeline()