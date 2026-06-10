"""
19_edge_labels.py — Edge Labels
Module 2, Phase 4: Organization & Dynamic

ใส่ Label บนเส้น Dependency ใน Graph View
เหมาะสำหรับ Branching เพื่อระบุเงื่อนไขแต่ละเส้นทาง
"""
from airflow.sdk import dag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
import pendulum


@dag(
    dag_id="19_edge_labels",
    schedule="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def edge_labels_demo():
    ingest = EmptyOperator(task_id="ingest")
    analyse = EmptyOperator(task_id="analyze")
    check = EmptyOperator(task_id="check_integrity")
    save = EmptyOperator(task_id="save")
    describe = EmptyOperator(task_id="describe_integrity")

    ingest >> analyse >> check
    check >> Label("No errors") >> save
    check >> Label("Errors found") >> describe


edge_labels_demo()