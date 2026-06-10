"""
21_dynamic_dags.py — Dynamic DAGs
Module 2, Phase 4: Organization & Dynamic

ใช้ Python loops/functions เพื่อสร้าง Tasks แบบ Dynamic
"""
from datetime import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.empty import EmptyOperator


@dag(
    dag_id="21_dynamic_dags",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["dynamic"],
)
def dynamic_dags():
    first = EmptyOperator(task_id="first")
    last = EmptyOperator(task_id="last")

    options = ["branch_a", "branch_b", "branch_c", "branch_d"]

    for option in options:
        t = EmptyOperator(task_id=option)
        first >> t >> last


dynamic_dags()
