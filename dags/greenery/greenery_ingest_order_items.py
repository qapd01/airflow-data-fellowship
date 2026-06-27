"""
Greenery Order Items Ingest Pipeline
====================================
CSV (local) → MinIO raw/ (CSV) → MinIO processed/ (Parquet) → ClickHouse greenery.order_items
"""

from datetime import datetime
from io import BytesIO

import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.sdk import dag, task, Asset
from greenery.schemas.order_items import pyarrow_schema, clickhouse_ddl

order_items_asset = Asset("clickhouse://greenery/order_items")

MINIO_CONN_ID = "minio_default"
MINIO_BUCKET = "greenery"

CSV_PATH = "/opt/airflow/docs/greenery/order_items.csv"

CLICKHOUSE_CONN_ID = "clickhouse_conn"
CLICKHOUSE_DB = "greenery"
CLICKHOUSE_TABLE = "order_items"


@dag(
    schedule="@daily",
    dag_id="greenery_ingest_order_items",
    start_date=datetime(2026, 1, 1),
    tags=["greenery", "ingest", "order_items"],
    catchup=False,
    doc_md=__doc__,
)
def greenery_ingest_order_items():
    """Load order_items.csv → MinIO raw → Parquet → MinIO processed → ClickHouse."""

    @task
    def upload_csv_to_minio_raw(ds=None) -> str:
        """Read order_items.csv from local filesystem and upload to MinIO raw layer."""
        raw_key = f"raw/order_items/{ds}/order_items.csv"
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

        if not s3_hook.check_for_bucket(bucket_name=MINIO_BUCKET):
            s3_hook.create_bucket(bucket_name=MINIO_BUCKET)
            print(f"📁 Created bucket '{MINIO_BUCKET}'")

        s3_hook.load_file(
            filename=CSV_PATH,
            key=raw_key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )
        print(f"✅ Uploaded CSV to s3://{MINIO_BUCKET}/{raw_key}")
        return raw_key

    @task
    def csv_to_parquet(raw_key: str, ds=None) -> str:
        """Download raw CSV from MinIO, convert to Parquet, upload to processed layer."""
        processed_key = f"processed/order_items/{ds}/order_items.parquet"
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

        csv_obj = s3_hook.get_key(key=raw_key, bucket_name=MINIO_BUCKET)
        csv_bytes = csv_obj.get()["Body"].read()

        convert_options = pcsv.ConvertOptions(column_types=pyarrow_schema())
        table = pcsv.read_csv(BytesIO(csv_bytes), convert_options=convert_options)

        print(f"📊 Parsed {table.num_rows} rows, {table.num_columns} columns")

        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)

        s3_hook.load_bytes(
            bytes_data=parquet_buffer.getvalue(),
            key=processed_key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )
        print(f"✅ Uploaded Parquet to s3://{MINIO_BUCKET}/{processed_key}")
        return processed_key

    @task
    def create_clickhouse_table():
        """Create greenery database and order_items table in ClickHouse if not exists."""
        ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        client = ch_hook.get_client()
        client.query(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

        client.query(
            clickhouse_ddl(CLICKHOUSE_DB, CLICKHOUSE_TABLE)
        )
        print(f"✅ Table {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} ready")

    @task(outlets=[order_items_asset])
    def load_parquet_to_clickhouse(processed_key: str):
        """Download Parquet from MinIO processed layer and insert into ClickHouse."""
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        parquet_obj = s3_hook.get_key(key=processed_key, bucket_name=MINIO_BUCKET)
        parquet_bytes = parquet_obj.get()["Body"].read()

        table = pq.read_table(BytesIO(parquet_bytes))
        print(f"📊 Read {table.num_rows} rows from Parquet")

        columns = table.column_names
        rows = []
        for i in range(table.num_rows):
            row = tuple(table.column(col)[i].as_py() for col in columns)
            rows.append(row)

        ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        client = ch_hook.get_client()

        client.query(f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")

        client.insert(
            f"{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}",
            rows,
            column_names=columns
        )

        result = client.query(f"SELECT count() FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")
        count = result.result_rows[0][0]
        print(f"✅ Loaded {count} rows into {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")

    # DAG task dependencies
    raw_key = upload_csv_to_minio_raw()
    processed_key = csv_to_parquet(raw_key)
    table_ready = create_clickhouse_table()
    table_ready >> load_parquet_to_clickhouse(processed_key)


greenery_ingest_order_items()
