"""
Module 2 Phase 2 — Schedules: Preset
=======================================
Demonstrates using Airflow's preset schedule strings:
  @daily, @hourly, @weekly, @monthly, @yearly, @once, @continuous

This DAG uses @daily with catchup=True to show how Airflow
creates DAG Runs for all past intervals since start_date.
"""

from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    dag_id="06_schedules_preset",
    start_date=datetime(year=2026, month=5, day=1, tz="Asia/Bangkok"),
    schedule="@daily",
    is_paused_upon_creation=False,  # This will make the DAG active immediately after creation
    catchup=True,  # This will allow the DAG to run for all past scheduled intervals since the start_date if it was not active before
)
def first_schedule_dag():

    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task
    def third_task():
        print("This is the third task. DAG complete!")

    # Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


# Instantiating the DAG
first_schedule_dag()
