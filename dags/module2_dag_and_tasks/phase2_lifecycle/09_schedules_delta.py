"""
Module 2 Phase 2 — Schedules: Delta (Interval-based)
======================================================
Demonstrates using DeltaTriggerTimetable for interval-based scheduling.
This DAG runs every 3 days instead of using a cron expression.
"""

from airflow.sdk import dag, task
from airflow.timetables.trigger import DeltaTriggerTimetable
from pendulum import datetime, duration


@dag(
    dag_id="09_schedules_delta",
    start_date=datetime(year=2026, month=5, day=1, tz="Asia/Bangkok"),
    schedule=DeltaTriggerTimetable(duration(days=3)),
    catchup=True,
)
def delta_schedule_dag():

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
delta_schedule_dag()
