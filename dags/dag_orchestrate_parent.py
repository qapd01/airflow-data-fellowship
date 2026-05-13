from posix import wait

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from dag_orchestrate_1 import dag_orchestrate_1
from dag_orchestrate_2 import dag_orchestrate_2

### this code is show it not work it need to use TriggerDagRunOperator to trigger the child DAGs instead of calling the DAG functions directly
# @dag(
#     dag_id="dag_orchestrate_parent",
# )
# def dag_orchestrate_parent():

#     @task
#     def first_orchestator_task():
#         return dag_orchestrate_1()

#     @task
#     def second_orchestrator_task():
#         return dag_orchestrate_2()

#     first_orchestator_task() >> second_orchestrator_task()


# # Instantiating the DAG
# dag_orchestrate_parent()
#
@dag(
    dag_id="dag_orchestrate_parent",
)
def dag_orchestrate_parent():

    trigger_dag_1 = TriggerDagRunOperator(
        task_id="trigger_dag_orchestrate_1",
        trigger_dag_id="dag_orchestrate_1",  # The DAG ID to trigger
        wait_for_completion=True,  # Wait for the triggered DAG to complete before proceeding -> after add this it change from async to sync
    )

    trigger_dag_2 = TriggerDagRunOperator(
        task_id="trigger_dag_orchestrate_2",
        trigger_dag_id="dag_orchestrate_2",  # The DAG ID to trigger
        wait_for_completion=True,  # Wait for the triggered DAG to complete before proceeding
    )

    trigger_dag_1 >> trigger_dag_2


# Instantiating the DAG
dag_orchestrate_parent()
