variable "credentials" {
  description = "Path to GCP Service Account Key"
  default     = "./keys/credentials.json"
}

variable "project" {
  description = "GCP Project ID"
  default     = "thai25pm-data-platform"
}

variable "region" {
  description = "GCP Region"
  default     = "asia-southeast1"
}

variable "location" {
  description = "Project Location for BQ and GCS"
  default     = "ASIA-SOUTHEAST1"
}

variable "gcs_bucket_name" {
  description = "My GCS Bucket Name"
  default     = "thai25pm-data-lake-bucket-252026"
}

variable "bq_dataset" {
  description = "BigQuery Dataset for Raw/Staging Data"
  default     = "thai25pm_dataset_252026"
}

variable "bq_dataset_prod" {
  description = "BigQuery Dataset for Production Data"
  default     = "thai25pm_data_prod_252026"
}