"""
23_jinja_templating.py — Jinja Templating & Context
Module 2, Phase 5: Advanced Interactions

Airflow ใช้ Jinja Templating ร่วมกับ Macros
เพื่อส่ง runtime values เข้าไปใน Operators
"""
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime


@dag(
    dag_id="23_jinja_templating",
    start_date=datetime(2026, 6, 6),
    schedule="@daily",
    catchup=False,
    tags=["jinja"],
)
def jinja_templating_demo():

    # ตัวอย่าง 1: ใช้ Jinja template ใน BashOperator
    bash_with_template = BashOperator(
        task_id="bash_with_template",
        bash_command=(
            "echo 'Data Interval Start: {{ ds }}' && "
            "echo 'Data Interval End: {{ ds_nodash }}' && "
            "echo 'Logical Date: {{ logical_date }}' && "
            "echo 'DAG ID: {{ dag.dag_id }}' && "
            "echo 'Task ID: {{ task.task_id }}'"
        ),
    )

    # ตัวอย่าง 2: ส่ง Jinja template เป็น env variable
    date = "{{ ds }}"
    bash_with_env = BashOperator(
        task_id="bash_with_env",
        bash_command="echo 'Processing data for date: $DATA_INTERVAL_START'",
        env={"DATA_INTERVAL_START": date},
    )

    # ตัวอย่าง 3: ใช้ context ใน @task
    @task
    def python_with_context(**context):
        ds = context.get("ds")
        logical_date = context.get("logical_date")
        dag_id = context.get("dag").dag_id

        print(f"📅 ds: {ds}")
        print(f"📅 logical_date: {logical_date}")
        print(f"📋 DAG ID: {dag_id}")

    bash_with_template >> bash_with_env >> python_with_context()


jinja_templating_demo()
