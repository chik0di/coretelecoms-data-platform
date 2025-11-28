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
