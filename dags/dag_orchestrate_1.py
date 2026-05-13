import os

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="dag_orchestrate_1",
)
def dag_orchestrate_1():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        os.makedirs(os.path.dirname("/opt/airflow/logs/data"), exist_ok=True)

        # Simulate data fetching and write to a file
        with open("/opt/airflow/logs/data/output_1.txt", "w") as f:
            f.write(f"Data procressed at {pendulum.now('Asia/Bangkok')}\n")

    # Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


# Instantiating the DAG
dag_orchestrate_1()
