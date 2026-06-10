"""
Module 2 Phase 1 — Operators
==============================
Demonstrates different operator types:
- @task.python  (PythonOperator via TaskFlow API)
- @task.bash    (BashOperator via TaskFlow API)
- BashOperator  (classic operator style)
"""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


@dag(
    dag_id="03_operators",
)
def operator_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task.bash
    def run_bash():
        return "echo https://airflow.apache.org/"

    run = BashOperator(
        task_id="run_this", bash_command="echo https://airflow.apache.org/"
    )

    # Defining task dependencies
    first = first_task()
    second = second_task()
    bash = run_bash()

    first >> second >> bash >> run


# Instantiating the DAG
operator_dag()
