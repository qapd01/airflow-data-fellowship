"""
DAG: dbt_greenery

Runs the dbt greenery project using Astronomer Cosmos.
Cosmos auto-generates Airflow tasks from dbt models with proper dependency ordering.

Project path inside Docker: /opt/airflow/dbt/greenery
"""

from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig

DBT_PROJECT_PATH = Path("/opt/airflow/dbt/greenery")

profile_config = ProfileConfig(
    profile_name="greenery",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

render_config = RenderConfig(
    select=[
        "my_first_dbt_model",
        "my_second_dbt_model"
    ]
)

from airflow.sdk import Asset

assets = [
    Asset("clickhouse://greenery/addresses"),
    Asset("clickhouse://greenery/events"),
    Asset("clickhouse://greenery/order_items"),
    Asset("clickhouse://greenery/orders"),
    Asset("clickhouse://greenery/products"),
    Asset("clickhouse://greenery/promos"),
    Asset("clickhouse://greenery/users"),
]

dbt_greenery_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=profile_config,
    render_config=render_config,
    dag_id="dbt_greenery",
    schedule=assets,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["dbt", "cosmos", "clickhouse", "greenery"],
    default_args={
        "retries": 1,
    },
)
