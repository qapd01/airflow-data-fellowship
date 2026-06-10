"""
17_setup_teardown.py — Setup and Teardown Tasks
Module 2, Phase 3: Control Flow & Branching

Setup: เตรียม Resource ก่อนเริ่มงาน
Teardown: ลบ Resource หลังจบงาน (รันเสมอแม้งานหลักพัง)
"""
from airflow.sdk import dag, task, setup, teardown
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime


@dag(
    dag_id="17_setup_teardown_demo",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["setup_teardown"],
)
def setup_teardown_demo():

    @setup
    def create_cluster():
        print("🔧 Setup: กำลังสร้าง Cluster...")
        return "cluster-001"

    @task
    def run_query_1(cluster_id):
        print(f"📊 Running Query 1 on {cluster_id}")
        return "query_1_result"

    @task
    def run_query_2(cluster_id):
        print(f"📊 Running Query 2 on {cluster_id}")
        return "query_2_result"

    @teardown
    def delete_cluster():
        print("🗑️ Teardown: กำลังลบ Cluster เพื่อประหยัดค่าใช้จ่าย...")

    # กำหนด Setup/Teardown relationships
    cluster = create_cluster()
    cleanup = delete_cluster()

    q1 = run_query_1(cluster)
    q2 = run_query_2(cluster)

    # ใช้ as_teardown เพื่อให้ cleanup รันเสมอแม้ query พัง
    cluster >> [q1, q2] >> cleanup


setup_teardown_demo()
