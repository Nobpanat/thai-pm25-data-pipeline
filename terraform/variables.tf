variable "credentials" {
  description = "Path to GCP Service Account Key"
  type        = string
  default     = "./keys/credentials.json"
}

variable "project" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
}

variable "location" {
  description = "Project Location for BQ and GCS"
  type        = string
}

variable "gcs_bucket_name" {
  description = "My GCS Bucket Name"
  type        = string
}

variable "bq_dataset" {
  description = "BigQuery Dataset for Raw/Staging Data"
  type        = string
}

variable "bq_dataset_prod" {
  description = "BigQuery Dataset for Production Data"
  type        = string
}