import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

SCRIPTS_PATH = "/opt/airflow/scripts"

OWNER = os.getenv("AIRFLOW_DAG_OWNER", "nite")
MAX_WORKERS = 8
LOOKBACK_DAYS = 7
MASTER_LOCATIONS_CSV = "/opt/airflow/data/thai_locations.csv"


def _add_scripts_path():
    if SCRIPTS_PATH not in sys.path:
        sys.path.append(SCRIPTS_PATH)


def sync_locations_task(csv_path):
    _add_scripts_path()
    from refresh_master_locations import sync_locations

    return sync_locations(csv_path=csv_path)


def run_daily_extract_task(execution_date, s3_bucket, csv_path, max_workers, lookback_days):
    _add_scripts_path()
    from airflow.sdk import Variable
    from daily_extract_s3_to_gcs import run_daily_extract

    gcs_bucket = Variable.get(
        "gcp_gcs_bucket",
        default=os.getenv("GCP_GCS_BUCKET"),
    )

    return run_daily_extract(
        execution_date=execution_date,
        s3_bucket=s3_bucket,
        gcs_bucket=gcs_bucket,
        csv_path=csv_path,
        max_workers=max_workers,
        lookback_days=lookback_days,
    )

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
        python_callable=sync_locations_task,
        op_kwargs={
            "csv_path": MASTER_LOCATIONS_CSV,
        },
    )

    extract_s3 = PythonOperator(
        task_id="extract_s3_to_gcs",
        python_callable=run_daily_extract_task,
        op_kwargs={
            "execution_date": "{{ ds }}",
            "s3_bucket": "openaq-data-archive",
            "csv_path": MASTER_LOCATIONS_CSV,
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
