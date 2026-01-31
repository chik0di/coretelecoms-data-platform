# CoreTelecoms Unified Customer Experience Data Platform

## Overview
CoreTelecoms — a major US telecom operator — is facing a customer retention crisis. Thousands of complaints arrive daily across multiple fragmented systems:

- Social Media

- Call Center logs

- Website Complaint Forms

- Customer Master Data

- Customer Service Agent Records

The business suffers from:

- Delayed reporting
- Manual spreadsheet-based analytics
- Siloed teams
- No unified view of customer behavior and complaints
- High customer churn

### The Mission

I was brought in as a Data Engineer to build a complete, production-quality Unified Customer Experience Data Platform that ingests all sources, centralizes storage, enforces data quality, models data with dbt, orchestrate workflows with Airflow, and provides a single source of truth for analytics & machine learning.

If this platform works, the company stands to save millions in churn reduction and customer retention.

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

    - Build a scalable DW (Snowflake)

    - Use dbt for better transformations, lineage, and testing

    - Publish trusted datasets for the Analytics team.

4. Orchestration (Apache Airflow)

    - Automate extraction

    - Automate transformations

    - Implement retries, idempotency, incremental loads, slack alerts

5. Containerization

    - Package entire project into Docker images

    - Push to cloud container registry

6. CI/CD

    - Automated lint checks

    - Automated Docker image builds and pushes

    - Deployment workflow

7. Infrastructure as Code

    - Terraform-managed AWS resources

    - IAM roles, S3 buckets

## Project Structure
```
coretelecoms-data-platform/
│
├── .github/
│   └── workflows/
│       └── ci.yml                 # CI pipeline for linting, testing, and validation
│
├── airflow/
│   ├── dags/                      # All DAG definitions for orchestration
│   ├── docker-compose.yaml        # Official Airflow Docker Compose setup
│   ├── Dockerfile                 # Custom Airflow image with providers (Docker, snowflake, AWS, etc.)
│   └── requirements.txt           # Python dependencies for Airflow runtime
│
├── dbt/
│   ├── core_telecom_dbt/          # dbt project (models, seeds, snapshots)
│   └── Dockerfile                 # Custom dbt image for containerized transforms
│
├── docs/
│   └── ...                        # Documentation files for the platform (Presentation, notes, etc)
│
├── infra/
│   └── ...                        # Infrastructure-as-code scripts (Terraform)
│
├── src/
│   ├── extract/                   # Extract scripts (Agent extraction, S3 ingestion, database pulls)
│   └── utils/                     # Shared utilities (postgres utilities, S3 utilities, slack alerts, config file, etc)
│
├
│
├── architecture.drawio.png        # System architecture diagram 
├
├── Dockerfile                     # Root-level Dockerfile (used future containerization)
└── README.md                      # Main project documentation

```

## [Architecture Diagram](architecture.drawio.png)

## Tools & Technologies
<p align="center">
    <a href="./src/utils/s3_utils.py">
        <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/amazonwebservices/amazonwebservices-plain-wordmark.svg" width="70" alt="AWS" />
    </a>
    &nbsp;&nbsp;&nbsp;
    <a href="./airflow/">
        <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/apacheairflow/apacheairflow-original.svg" width="70" alt="Airflow" />
    </a>
    &nbsp;&nbsp;&nbsp;
    <a href="./Dockerfile">
        <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/docker/docker-original.svg" width="70" alt="Docker" />
    </a>
    &nbsp;&nbsp;&nbsp;
    <a href="./infra/">
        <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/terraform/terraform-original.svg" width="70" alt="Terraform" />
    </a>
    &nbsp;&nbsp;&nbsp;
    <a href="./src/extract/">
        <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" width="70" alt="Python" />
    </a>
    &nbsp;&nbsp;&nbsp;
    </a href="./snowflake">
        <img width="70" src="https://img.icons8.com/water-color/50/snowflake.png" alt="snowflake"/>
    </a>
    &nbsp;&nbsp;&nbsp;
    </a href="./dbt/">
        <img width="70" src="image.png" alt="dbt"/>
    </a>
</p>
