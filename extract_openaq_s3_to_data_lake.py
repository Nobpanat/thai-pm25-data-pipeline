"""
Extract OpenAQ S3 to Data Lake (GCS)
=====================================
Performs a historical backfill by transferring raw data
from the public OpenAQ S3 archive to our Google Cloud Storage data lake.
Only transfers files that do not already exist in GCS (idempotent).

Usage:
    uv run python extract_openaq_s3_to_data_lake.py
"""

import os
import logging
from dotenv import load_dotenv
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from google.cloud import storage
import pandas as pd
import sys
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configuration
S3_BUCKET_NAME = os.getenv("S3_SOURCE_BUCKET", "openaq-data-archive")
GCS_BUCKET_NAME = os.getenv("GCP_GCS_BUCKET")
if not GCS_BUCKET_NAME:
    logger.error("GCP_GCS_BUCKET is not set in environment variables.")
    sys.exit(1)
SERVICE_ACCOUNT_JSON_PATH = "terraform/keys/credentials.json"
CSV_PATH = "data/thai_locations.csv"

START_YEAR = int(os.getenv("START_YEAR", 2018))
END_YEAR = int(os.getenv("END_YEAR", 2026))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 25))

s3_client = boto3.client(
    "s3", config=Config(signature_version=UNSIGNED, max_pool_connections=50)
)
gcs_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_JSON_PATH)
gcs_bucket = gcs_client.bucket(GCS_BUCKET_NAME)


def transfer_single_file(args):
    s3_key, gcs_blob_name = args
    try:
        blob = gcs_bucket.blob(gcs_blob_name)
        if not blob.exists():
            s3_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            blob.upload_from_file(s3_obj["Body"])
        return True
    except Exception as e:
        logger.error(f"Failed to transfer {s3_key}: {e}")
        return f"Error {s3_key}: {e}"


def backfill_openaq_data():
    logger.info(f"Starting Parallel Backfill: {START_YEAR}-{END_YEAR}")

    try:
        df_locations = pd.read_csv(CSV_PATH)
        location_ids = sorted(df_locations["location_id"].dropna().unique().astype(int))

        # Scan files
        all_transfer_tasks = []
        logger.info("Scanning S3 for file list...")

        for loc_id in tqdm(location_ids, desc="Scanning Locations"):
            for year in range(START_YEAR, END_YEAR + 1):
                s3_prefix = f"records/csv.gz/locationid={loc_id}/year={year}/"

                paginator = s3_client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=s3_prefix):
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            s3_key = obj["Key"]

                            parts = s3_key.split("/")
                            month_part = parts[-2]
                            month = month_part.split("=")[-1]
                            filename = parts[-1]

                            gcs_name = f"openaq_data/location_{loc_id}/{year}/{month}/{filename}"
                            all_transfer_tasks.append((s3_key, gcs_name))

        total_files = len(all_transfer_tasks)
        logger.info(f"Total files found: {total_files}")

        logger.info(f"Transferring using {MAX_WORKERS} workers...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            list(
                tqdm(
                    executor.map(transfer_single_file, all_transfer_tasks),
                    total=total_files,
                    desc="Transfer Progress",
                    unit="file",
                )
            )

        logger.info("All locations processed successfully!")

    except Exception as e:
        logger.critical(f"Fatal Error: {e}", exc_info=True)


if __name__ == "__main__":
    backfill_openaq_data()
