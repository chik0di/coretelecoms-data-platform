import boto3
from sqlalchemy import create_engine, text
import pandas as pd 
import datetime

def get_db_credentials(param_name):
    ssm = boto3.client("ssm")
    response = ssm.get_parameter(Name=param_name, WithDecryption=True)
    return response["Parameter"]["Value"]

host = get_db_credentials(param_name="/coretelecomms/database/db_host")
database = get_db_credentials(param_name="/coretelecomms/database/db_name")
user = get_db_credentials(param_name="/coretelecomms/database/db_username")
port = get_db_credentials(param_name="/coretelecomms/database/db_port")
password = get_db_credentials(param_name="/coretelecomms/database/db_password")
schema = get_db_credentials(param_name="/coretelecomms/database/table_schema_name")

engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
)

def list_webform_tables():
    query = text(f"""
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_name LIKE 'web_form_request_%'
        ORDER BY table_name;
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    return [r[0] for r in rows]


# s3 = boto3.client("s3")
# def s3_partition_exists(bucket: str, prefix: str) -> bool:
#     """Check if any object exists in S3 under the given prefix."""
#     response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
#     return 'Contents' in response

def generate_s3_partitioned_key(prefix: str, table) -> str:
    """
    Generate a partitioned S3 key based on the date inside the tablename.
    Expects a tablename formatted like: web_form_request_2025_11_20
    """
    tablename = table             
    date_str = tablename.split("_")[3:]  # 2025-11-20
    year, month, day = date_str[0], date_str[1], date_str[2]
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parquet_name = f"webform_{ts}.parquet"
    return f"{prefix}/{year}/{month}/{parquet_name}"