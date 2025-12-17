from airflow import DAG
from airflow import DAG
from airflow.decorators import task
from airflow.decorators import task_group
from datetime import timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from src.extract.extract_customers_s3 import run as ingest_customers_data
from src.extract.extract_agents_sheets import run as ingest_agents_data
from src.extract.extract_call_logs_s3 import run_folder as ingest_call_logs_data
from src.extract.extract_sm_s3 import run_folder as ingest_social_media_data
from src.extract.extract_webforms_pg import extract_and_upload_forms as ingest_webforms_data
from src.utils.slack_alerts import slack_failure_alert, slack_success_alert


default_args = {
    'owner':'airflow',
    'retries':2,
    'retry_delay':timedelta(minutes=1),
    'on_failure_callback': slack_failure_alert,
    'on_success_callback': slack_success_alert,
}

with DAG(
    dag_id='data_orchestration_dag',
    default_args=default_args,
    description='Orchestrate data ingestion from various sources to S3',
    schedule=None,
    catchup=False,
    template_searchpath=['/opt/airflow/snowflake/']
) as dag:
    
    @task_group(group_id="data_ingestion_tasks")
    def data_ingestion_tasks():

        @task
        def ingest_customers_task():
            ingest_customers_data(s3_key="customers/customers_dataset.csv", write_back_to_s3=True)

        @task
        def ingest_agents_task():
            ingest_agents_data(write_back_to_s3=True)

        @task
        def ingest_call_logs_task():
            ingest_call_logs_data(prefix="call logs/")

        @task
        def ingest_social_media_task():
            ingest_social_media_data(prefix="social_medias/")

        @task
        def ingest_webforms_task():
            ingest_webforms_data(write_back_to_s3=True)

        
        ingest_customers_task()
        ingest_agents_task()
        ingest_call_logs_task()
        ingest_social_media_task()
        ingest_webforms_task()

    load_into_snowflake = SnowflakeSqlApiOperator(
    task_id="load_into_snowflake",
    snowflake_conn_id="snowflake_conn",
    sql="copy_into_staging.sql"
    )

    mounts = [
        Mount(
            target="/root/.dbt",  
            source="C:/Users/PC/Documents/airflow/dags/core_telecomms_dbt",
            type="bind",
            read_only=True
        )
    ]

    data_modeling_task = DockerOperator(
        task_id="data_modeling_task",
        docker_conn_id="docker_conn",
        image="chik0di/core-telecom-data-modeling:latest",
        command="run",
        docker_url="tcp://host.docker.internal:2375",
        api_version="auto",
        network_mode="bridge",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=mounts
    )


    data_ingestion_tasks() >> load_into_snowflake >> data_modeling_task
