from airflow import DAG
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta

from src.extract.extract_customers_s3 import run as ingest_customers_data
from src.extract.extract_agents_sheets import run as ingest_agents_data
from src.extract.extract_call_logs_s3 import run_folder as ingest_call_logs_data
from src.extract.extract_sm_s3 import run_folder as ingest_social_media_data
from src.extract.extract_webforms_pg import extract_and_upload_forms as ingest_webforms_data
from src.utils.slack_alerts import slack_failure_alert, slack_success_alert


default_args = {
    'owner':'airflow',
    # 'retries':2,
    'retry_delay':timedelta(minutes=1),
    'on_failure_callback': slack_failure_alert,
    'on_success_callback': slack_success_alert
}

with DAG(
    dag_id='data_orchestration_dag',
    default_args=default_args,
    description='Orchestrate data ingestion from various sources to S3',
    schedule=None,
    catchup=False
) as dag:

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

    customers = ingest_customers_task()
    agents = ingest_agents_task()
    call_logs = ingest_call_logs_task()
    social_media = ingest_social_media_task()
    webforms = ingest_webforms_task()

    [customers, agents, call_logs, social_media, webforms]