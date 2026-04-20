import os
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/scripts")
from refresh_master_locations import sync_locations
from daily_extract_s3_to_gcs import run_daily_extract

OWNER = os.getenv("AIRFLOW_DAG_OWNER", "nite")
MAX_WORKERS = 8
LOOKBACK_DAYS = 7

default_args = {
    "owner": OWNER,
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="thai_pm25_daily_v1",
    default_args=default_args,
    description="Daily Incremental Load from OpenAQ S3 to GCS",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=["thai-pm25", "openaq"],
) as dag:

    update_master = PythonOperator(
        task_id="update_master_locations",
        python_callable=sync_locations,
        op_kwargs={
            "csv_path": "/opt/airflow/data/thai_locations.csv",
        },
    )

    extract_s3 = PythonOperator(
        task_id="extract_s3_to_gcs",
        python_callable=run_daily_extract,
        op_kwargs={
            "execution_date": "{{ ds }}",
            "s3_bucket": "openaq-data-archive",
            "gcs_bucket": Variable.get(
                "gcp_gcs_bucket",
                default=os.getenv("GCP_GCS_BUCKET"),
            ),
            "csv_path": "/opt/airflow/data/thai_locations.csv",
            "max_workers": MAX_WORKERS,
            "lookback_days": LOOKBACK_DAYS,
        },
    )

    copy_seed_file = BashOperator(
        task_id="copy_seed_file",
        bash_command="cp /opt/airflow/data/thai_locations.csv /opt/airflow/dbt_thai_pm25/seeds/thai_locations.csv",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="dbt seed --profiles-dir .",
        cwd="/opt/airflow/dbt_thai_pm25",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --profiles-dir .",
        cwd="/opt/airflow/dbt_thai_pm25",
    )

    update_master >> extract_s3 >> copy_seed_file >> dbt_seed >> dbt_run
