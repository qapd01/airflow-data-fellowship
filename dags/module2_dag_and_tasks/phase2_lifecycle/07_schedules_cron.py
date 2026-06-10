"""
Module 2 Phase 2 — Schedules: Cron Expression
===============================================
Demonstrates using a cron expression with CronTriggerTimetable
for timezone-aware scheduling.

This DAG runs at 16:00 (4 PM) Monday–Friday in Asia/Bangkok timezone.
"""

from airflow.sdk import dag, task
from airflow.timetables.trigger import CronTriggerTimetable
from pendulum import datetime


@dag(
    dag_id="07_schedules_cron",
    start_date=datetime(year=2026, month=5, day=1, tz="Asia/Bangkok"),
    schedule=CronTriggerTimetable(cron="0 16 * * MON-FRI", timezone="Asia/Bangkok"),
    catchup=True,
)
def cron_schedule_dag():

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
cron_schedule_dag()
