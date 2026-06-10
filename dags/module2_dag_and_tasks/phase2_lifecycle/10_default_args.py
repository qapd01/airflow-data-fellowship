"""
Module 2 Phase 2 — Default Arguments
=======================================
Demonstrates how to use default_args to apply common parameters
to all operators within a DAG, instead of repeating them in each operator.

From the curriculum (lines ~405-445):
  default_args are applied automatically to every operator in the DAG.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

# ──────────────────────────────────────────────
# Example 1: Full default_args dictionary
# ──────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# ──────────────────────────────────────────────
# Example 2: Using default_args in a DAG
# ──────────────────────────────────────────────
@dag(
    dag_id="10_default_args",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    schedule="@daily",
    default_args={"retries": 2},
    catchup=False,
    tags=["module2", "phase2", "default_args"],
)
def default_args_demo():
    op = BashOperator(
        task_id="hello_world",
        bash_command="echo 'Hello World!'",
    )

    print(op.retries)  # 2


default_args_demo()
