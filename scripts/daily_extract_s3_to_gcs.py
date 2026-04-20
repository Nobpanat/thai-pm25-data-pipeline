import boto3
from botocore.exceptions import ClientError
from botocore import UNSIGNED
from botocore.config import Config
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk.exceptions import AirflowSkipException
import pandas as pd
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor


def transfer_single_file(s3_client, gcs_bucket, s3_bucket_name, s3_key, gcs_blob_name):
    logger = logging.getLogger("airflow.task")
    try:
        blob = gcs_bucket.blob(gcs_blob_name)
        if not blob.exists():
            try:
                s3_obj = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key)
                blob.upload_from_string(
                    s3_obj["Body"].read(), content_type="application/x-gzip"
                )
                return True
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    return False
                raise e
        return False
    except Exception as e:
        logger.error(f"Failed to transfer {s3_key}: {e}")
        return False


def run_daily_extract(execution_date, **kwargs):
    logger = logging.getLogger("airflow.task")

    s3_bucket_name = kwargs.get("s3_bucket", "openaq-data-archive")
    gcs_bucket_name = kwargs.get("gcs_bucket")
    csv_path = kwargs.get("csv_path", "/opt/airflow/data/thai_locations.csv")
    max_workers = kwargs.get("max_workers", 8)
    lookback_days = kwargs.get("lookback_days", 7)

    if not gcs_bucket_name:
        raise ValueError("gcs_bucket is required!")

    base_dt = datetime.strptime(execution_date, "%Y-%m-%d")
    logger.info(f"Starting Optimized Daily Extract for: {execution_date}")

    # Init Clients
    s3_client = boto3.client(
        "s3", config=Config(signature_version=UNSIGNED, max_pool_connections=20)
    )
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    gcs_bucket = gcs_hook.get_conn().bucket(gcs_bucket_name)

    df_locations = pd.read_csv(csv_path)
    location_ids = df_locations["location_id"].unique().astype(int)

    all_tasks = []

    needed_months = set()
    for i in range(lookback_days):
        dt = base_dt - timedelta(days=i)
        needed_months.add((dt.year, f"{dt.month:02d}"))

    logger.info(f"Scanning S3 for {len(location_ids)} locations...")

    def scan_location_month(loc_id, year, month):
        tasks = []
        prefix = f"records/csv.gz/locationid={loc_id}/year={year}/month={month}/"
        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=s3_bucket_name, Prefix=prefix):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        s3_key = obj["Key"]
                        filename = s3_key.split("/")[-1]
                        gcs_name = (
                            f"openaq_data/location_{loc_id}/{year}/{month}/{filename}"
                        )

                        for d in range(lookback_days):
                            check_dt = base_dt - timedelta(days=d)
                            dt_str = check_dt.strftime("%Y%m%d")
                            if dt_str in filename:
                                tasks.append((s3_key, gcs_name))
                                break
        except Exception:
            pass
        return tasks

    scan_jobs = []
    for loc_id in location_ids:
        for year, month in needed_months:
            scan_jobs.append((loc_id, year, month))

    with ThreadPoolExecutor(max_workers=max_workers * 2) as scan_executor:
        futures = [scan_executor.submit(scan_location_month, *job) for job in scan_jobs]
        for f in futures:
            all_tasks.extend(f.result())

    all_tasks = list(set(all_tasks))

    if not all_tasks:
        logger.info("No files found after scanning.")
        raise AirflowSkipException("No data to transfer.")

    logger.info(f"Found {len(all_tasks)} files. Starting Transfer...")

    with ThreadPoolExecutor(max_workers=max_workers) as transfer_executor:
        results = [
            transfer_executor.submit(
                transfer_single_file, s3_client, gcs_bucket, s3_bucket_name, t[0], t[1]
            )
            for t in all_tasks
        ]

    success_count = sum(1 for r in results if r.result())

    if success_count == 0:
        logger.info(
            "Sync completed: No NEW files found or all files already exist in GCS."
        )
        raise AirflowSkipException(
            f"No new data found for the last {lookback_days} days. Skipping downstream dbt tasks to save costs."
        )

    logger.info(f"Sync completed: Successfully transferred {success_count} new files.")
