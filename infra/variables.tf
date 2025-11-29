variable "aws_region" {
  description = "AWS region to deploy infrastructure"
  type        = string
}

variable "project_name" {
  description = "Prefix for all resources"
  type        = string
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
}

variable "snowflake_iam_user_arn" {
  description = "snowflake generated user ARN"
  type        = string
}

variable "snowflake_external_id" {
  description = "Snowflale generated eternal id"
  type        = string
}

variable "bucket_resource_arn" {
  description = "ARN of S3 bucket"
  type        = string
}

variable "bucket_objects_arn" {
  description = "ARN of S3 bucket with a forward slash and an asterisk to include all of its objects"
  type        = string
}