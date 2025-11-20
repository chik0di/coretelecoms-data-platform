# CoreTelecoms Unified Customer Experience Data Platform

## Overview
CoreTelecoms, a leading telecom operator in the United States, is experiencing a major customer retention crisis due to poor handling of customer complaints coming from multiple channels such as social media, call center logs, and website forms. Data is scattered across different systems, stored in inconsistent formats, and compiled manually by reporting teams; leading to delays, poor insights, and customer churn.

This project implements an end-to-end, production-grade data platform to unify all customer complaint data into a centralized, analytics-ready system for operational reporting, BI, and ML use cases.

## Project Objectives

This platform covers:

1. Data Ingestion (Raw Layer)

- Extract data from S3 (CSV/JSON), Google Sheets, and PostgreSQL tables

- Store them as raw Parquet files with metadata

2. Data Cleaning & Enrichment

- Standardize messy inputs

- Validate emails, normalize column naming

- Perform joins, enrichments, categorization, and data quality checks

3. Data Warehouse Modeling

- Build a scalable DW (Snowflake/Redshift/BigQuery)

- Use dbt for better transformations, lineage, and testing

- Publish trusted datasets for Analytics & ML teams

4. Orchestration (Apache Airflow)

- Automate extraction

- Automate transformations

- Implement retries, idempotency, incremental loads, alerts

5. Containerization

- Package extractors and DAGs into Docker images

- Push to cloud container registry

6. CI/CD

- Automated lint checks

- Automated Docker image builds and pushes

- Deployment workflow

7. Infrastructure as Code

- Terraform-managed AWS resources

- IAM roles, S3 buckets, networking

- Remote backend for state locking

## Architecture Diagram

## Project Structure

## Getting Started

## Tools & Technologies