"""
Module 2 Phase 2 — Incremental Load
======================================
Demonstrates using data_interval_start and data_interval_end
to implement incremental data loading — fetching only data
from the current interval window.

Uses CronDataIntervalTimetable with catchup=True to show
how each DAG Run processes a different data window.
"""

import pprint

from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime


@dag(
    dag_id="13_incremental_load",
    schedule=CronDataIntervalTimetable(
        cron="@daily", timezone="Asia/Bangkok"
    ),  # Daily at midnight
    start_date=datetime(year=2026, month=1, day=26, tz="Asia/Bangkok"),
    end_date=datetime(year=2026, month=1, day=31, tz="Asia/Bangkok"),
    catchup=True,
)
def incremental_load_dag():
    @task
    def show_all_context(**kwargs):

        print("--- Available Context Keys ---")
        for key in kwargs.keys():
            print(key)

        print("\n--- Context Values ---")
        pprint.pprint(kwargs)

    @task
    def incremental_data_fetch(**kwargs):
        data_interval_start = kwargs["data_interval_start"]
        data_interval_end = kwargs["data_interval_end"]
        print(f"Fetching data from {data_interval_start} to {data_interval_end}")

    @task.bash
    def incremental_data_process():
        return "echo 'Processing incremental data from {{data_interval_start}} to {{data_interval_end}}'"

    fetch = incremental_data_fetch()
    process = incremental_data_process()
    show_context = show_all_context()

    show_context >> fetch >> process


incremental_load_dag()
