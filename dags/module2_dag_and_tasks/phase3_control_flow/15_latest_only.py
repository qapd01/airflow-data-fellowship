"""
15_latest_only.py — LatestOnlyOperator
Module 2, Phase 3: Control Flow & Branching

LatestOnlyOperator จะ Skip Tasks ที่อยู่ Downstream
หากตรวจพบว่า DAG Run นั้นไม่ใช่รอบการรันล่าสุด
"""
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.sdk import dag, TriggerRule
import datetime
import pendulum


@dag(
    dag_id="15_latest_only_with_trigger",
    schedule=datetime.timedelta(hours=4),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example3"],
)
def latest_only_demo():
    latest_only = LatestOnlyOperator(task_id="latest_only")

    task1 = EmptyOperator(task_id="task1")
    task2 = EmptyOperator(task_id="task2")
    task3 = EmptyOperator(task_id="task3")
    task4 = EmptyOperator(
        task_id="task4",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    latest_only >> task1 >> [task3, task4]
    task2 >> [task3, task4]


latest_only_demo()