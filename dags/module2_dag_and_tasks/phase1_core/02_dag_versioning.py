"""
Module 2 Phase 1 — DAG Versioning
===================================
Airflow 3.x tracks DAG structure changes automatically.
A running DAG continues with its original version until completion,
even if a new version is uploaded.

Try uncommenting the version_task lines to create version 2.0.
"""

from airflow.sdk import dag, task


@dag(
    dag_id="02_dag_versioning",
)
def versioning_dag():

    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task
    def third_task():
        print("This is the third task. DAG complete!")

    ## New task added in version 2.0 of the DAG after show first version of the DAG in Airflow UI
    @task
    def version_task():
        print("This is the version task. This DAG has been updated to version 2.0!")

    # Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    ## New task added in version 2.0 of the DAG after show first version of the DAG in Airflow UI
    version = version_task()

    # first >> second >> third
    first >> second >> third >> version


# Instantiating the DAG
versioning_dag()
