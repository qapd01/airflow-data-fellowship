"""
Module 2 Phase 2 — Data Interval & Logical Date
==================================================
Demonstrates the core scheduling concepts:
- data_interval_start: beginning of the data window
- data_interval_end: end of the data window
- logical_date: label for the DAG Run (== data_interval_start, NOT the actual run time)

Key insight from curriculum:
  "Logical Date คือ 'ฉลาก' ของงาน — it identifies WHICH data window
  this run is responsible for, NOT when the run actually executes."
"""

from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime


@dag(
    dag_id="11_data_interval",
    schedule=CronDataIntervalTimetable(cron="@daily", timezone="Asia/Bangkok"),
    start_date=datetime(2026, 6, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["module2", "phase2", "data_interval"],
)
def data_interval_demo():
    """Print data_interval_start, data_interval_end, and logical_date."""

    @task
    def show_intervals(**kwargs):
        logical_date = kwargs["logical_date"]
        data_interval_start = kwargs["data_interval_start"]
        data_interval_end = kwargs["data_interval_end"]

        print("=" * 60)
        print("📅 DATA INTERVAL & LOGICAL DATE DEMO")
        print("=" * 60)
        print(f"  logical_date         : {logical_date}")
        print(f"  data_interval_start  : {data_interval_start}")
        print(f"  data_interval_end    : {data_interval_end}")
        print("-" * 60)
        print("💡 Note:")
        print("  • logical_date == data_interval_start")
        print("  • The DAG actually runs AFTER data_interval_end")
        print("  • For @daily, the run for June 1 executes on June 2 00:00")
        print("=" * 60)

    @task.bash
    def show_via_jinja():
        """Use Jinja templating to access the same values in a Bash task."""
        return (
            "echo 'Jinja — ds: {{ ds }}' && "
            "echo 'Jinja — data_interval_start: {{ data_interval_start }}' && "
            "echo 'Jinja — data_interval_end:   {{ data_interval_end }}'"
        )

    show_intervals() >> show_via_jinja()


data_interval_demo()
