"""
Module 2 Phase 2 — Schedules: Timetable + Timezone
=====================================================
Demonstrates using CronDataIntervalTimetable to handle
timezone-aware scheduling (Asia/Bangkok).

From the curriculum:
  If you use schedule="0 12 * * *" directly, Airflow treats it as UTC,
  meaning it runs at 7 PM Bangkok time (UTC+7)!
  The correct approach is to use a Timetable with explicit timezone.
"""

from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime


@dag(
    dag_id="08_schedules_timetable",
    # Use Timetable to specify Cron and set Timezone to Bangkok
    schedule=CronDataIntervalTimetable(cron="0 12 * * *", timezone="Asia/Bangkok"),
    start_date=datetime(2026, 5, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["module2", "phase2", "timetable", "timezone"],
)
def timetable_timezone_dag():
    """DAG that runs at 12:00 noon Bangkok time using CronDataIntervalTimetable."""

    @task
    def report_schedule(**kwargs):
        logical_date = kwargs["logical_date"]
        data_interval_start = kwargs["data_interval_start"]
        data_interval_end = kwargs["data_interval_end"]
        print(f"📅 Logical Date      : {logical_date}")
        print(f"⏰ Data Interval Start: {data_interval_start}")
        print(f"⏰ Data Interval End  : {data_interval_end}")
        print("✅ Running at 12:00 noon Bangkok time (not UTC)!")

    report_schedule()


timetable_timezone_dag()
