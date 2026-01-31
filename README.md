# CoreTelecoms Unified Customer Complaint Data Platform

## Overview
CoreTelecomms — the telecommunication subsidiary of CDE — is facing a customer retention crisis. Thousands of complaints arrive daily across multiple fragmented systems:

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

## Project Objective

To build a complete, Unified Customer Complaint Data Platform that ingests all sources, centralizes storage, enforces data quality, models data, orchestrate workflows, and provides a single source of truth for mainly analytics & subsequently, machine learning.

## What the Application Does

The CoreTelecomms Customer Complaint Analytics Platform is an end-to-end data pipeline and analytics system designed to ingest, standardize, analyze, and visualize customer complaints from multiple operational sources within a telecom environment.

The application workflow includes:

- Extracting raw complaint data from structured and semi-structured sources
- Cleaning and transforming inconsistent complaint records into analytics-ready datasets
- Modeling the data into fact and dimension tables optimized for analysis
- Delivering insights on complaint volume, service categories, customer segments, and escalation trends
- Scheduled ingestion, transformation, and loading of complaint data with task-level dependency management

The final output enables operations teams, customer experience managers, and executives to:

- Identify recurring customer pain points
- Make data-driven decisions to improve customer satisfaction and service reliability
  
In short, the application turns raw customer complaints into actionable intelligence.

## Project Setup Guide 
This project is currently being containerized with Docker to provide a consistent, portable runtime for Airflow, dbt, and all pipeline dependencies. Stay Tuned.

## Architectural Diagram


## Repo Structure
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

## [Architectural Diagram](architecture.drawio.png)
![Architecture Diagram](architecture.drawio.png)

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

## Additional Resources
[Creating and Using Custom Schemas in dbt](https://docs.getdbt.com/docs/build/custom-schemas)

[Creating an S3 Stage in Snowflake](https://docs.snowflake.com/en/user-guide/data-load-s3-create-stage)

[Copying data from an S3 Stage](https://docs.snowflake.com/en/user-guide/data-load-s3-copy)

[Troubleshooting Obscure task failures in Airflow](https://airflow.apache.org/docs/apache-airflow/stable/troubleshooting.html)

Using the SnowflakeSQLAPI Operator in Airflow- [Airflow Docs](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/operators/snowflake.html#snowflakesqlapioperator), [My article](https://medium.com/@chik0di/using-the-snowflakesqlapi-operator-in-airflow-0206632db2a3?postPublishedType=initial)
