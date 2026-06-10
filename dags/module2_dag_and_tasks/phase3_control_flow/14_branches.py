"""
14_branches.py — @task.branch & Trigger Rules
Module 2, Phase 3: Control Flow & Branching
"""
from airflow.sdk import dag, task


@dag(
    dag_id="14_branches_dag",
)
def branches_dag():

    @task
    def extract_task(**kwargs):
        print("Extracting Data...")

        ti = kwargs["ti"]
        extracted_data_dict = {
            "api_extracted_data": [1, 2, 3],
            "db_extracted_data": [4, 5, 6],
            "s3_extracted_data": [7, 8, 9],
            "weekend_flag": False,
        }

        ti.xcom_push(key="return_value", value=extracted_data_dict)

    @task
    def transform_task_api(**kwargs):
        ti = kwargs["ti"]
        api_extracted_data = ti.xcom_pull(
            task_ids="extract_task",
        )["api_extracted_data"]
        print(f"Transforming API data: {api_extracted_data}...")

        transform_api_data = [i * 10 for i in api_extracted_data]

        ti.xcom_push(key="return_value", value=transform_api_data)

    @task
    def transform_task_db(**kwargs):
        ti = kwargs["ti"]
        db_extracted_data = ti.xcom_pull(
            task_ids="extract_task",
        )["db_extracted_data"]
        print(f"Transforming DB data: {db_extracted_data}...")

        transform_db_data = [i * 100 for i in db_extracted_data]

        ti.xcom_push(key="return_value", value=transform_db_data)

    @task
    def transform_task_s3(**kwargs):
        ti = kwargs["ti"]
        s3_extracted_data = ti.xcom_pull(
            task_ids="extract_task",
        )["s3_extracted_data"]
        print(f"Transforming S3 data: {s3_extracted_data}...")

        transform_s3_data = [i * 1000 for i in s3_extracted_data]

        ti.xcom_push(key="return_value", value=transform_s3_data)

    # Create the Decider Task
    @task.branch
    def decider_task(**kwargs):
        ti = kwargs["ti"]
        weekend_flag = ti.xcom_pull(task_ids="extract_task")["weekend_flag"]

        if weekend_flag:
            return "no_load_data"
        else:
            return "load_task"

    @task.bash
    def load_task(**kwargs):
        print("Loading data...")
        api_data = kwargs["ti"].xcom_pull(task_ids="transform_task_api")
        db_data = kwargs["ti"].xcom_pull(task_ids="transform_task_db")
        s3_data = kwargs["ti"].xcom_pull(task_ids="transform_task_s3")

        return f"echo 'Loaded Data: {api_data}, {db_data}, {s3_data}'"

    @task.bash
    def no_load_data(**kwargs):
        print("No data to load since it's the weekend...")
        return "echo 'No Load Task Executed'"

    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    load = load_task()
    no_load = no_load_data()
    decider = decider_task()

    extract >> [transform_api, transform_db, transform_s3] >> decider
    decider >> [load, no_load]


# Instantiating the DAG
branches_dag()
