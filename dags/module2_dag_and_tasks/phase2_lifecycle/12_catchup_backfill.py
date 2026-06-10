"""
Module 2 Phase 2 — Catchup & Backfill
========================================
Demonstrates the difference between catchup=True and catchup=False.

From the curriculum:
  • catchup=True  → Airflow auto-creates DAG Runs for ALL past intervals
                    since start_date (e.g. sales reports that need historical data)
  • catchup=False → Airflow only creates a DAG Run for the LATEST interval
                    (e.g. real-time alerts that don't need history)

  Backfill is a separate manual process triggered via UI or CLI:
    airflow backfill create --dag-id DAG_ID --start-date ... --end-date ...
"""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from datetime import datetime

# ──────────────────────────────────────────────
# DAG 1: catchup=True
# ──────────────────────────────────────────────
# This DAG will backfill all missed runs from start_date to today.
# Suitable for: daily sales reports, historical data processing.
@dag(
    dag_id="12_catchup_backfill",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=True,  # Important: backfill all past intervals
    tags=["module2", "phase2", "catchup"],
)
def catchup_backfill():
    # This task runs once per day for every day since start_date
    BashOperator(
        task_id="generate_report",
        bash_command="echo 'Generating sales report for {{ ds }}'",
    )


catchup_backfill()
# ──────────────────────────────────────────────
# DAG 2: catchup=False
# ──────────────────────────────────────────────
# This DAG only runs for the most recent interval.
# Suitable for: system alerts, real-time notifications.
@dag(
    dag_id="12_system_alert_no_catchup",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,  # Important: ignore past intervals
    tags=["module2", "phase2", "catchup"],
)
def system_alert_no_catchup():
    # This task only runs from today onwards (no backfill)
    BashOperator(
        task_id="send_alert",
        bash_command="echo 'Sending alert for today: {{ ds }}'",
    )


system_alert_no_catchup()
