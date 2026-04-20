FROM apache/airflow:3.2.0

RUN pip install --no-cache-dir \
    pandas \
    google-cloud-bigquery \
    boto3 \
    google-cloud-storage \
    pytz \
    geopy \
    openaq \
    dbt-core \
    dbt-bigquery