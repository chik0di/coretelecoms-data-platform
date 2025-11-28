import boto3
from botocore.exceptions import ClientError
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import datetime
from .config import AWS_REGION, S3_RAW_BUCKET, S3_TARGET_BUCKET, TARGET_PROFILE, RAW_PROFILE

def raw_s3():
    session = boto3.Session(profile_name=RAW_PROFILE, region_name=AWS_REGION)
    return session.client("s3")

def target_s3():
    session = boto3.Session(profile_name=TARGET_PROFILE, region_name=AWS_REGION)
    return session.client("s3")

s3_source = raw_s3()
s3_dest = target_s3()

def list_objects(prefix: str = ""):
    prefix = prefix.lstrip("/")
    s3 = s3_source
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_RAW_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def read_csv_from_s3_to_df(s3_key: str, **pd_kwargs) -> pd.DataFrame:
    """
    this function reads a CSV file from S3 and returns it as a pandas DataFrame
    """
    try:
        s3 = s3_source
        resp = s3.get_object(Bucket=S3_RAW_BUCKET, Key=s3_key)
        body = resp["Body"].read()
        if s3_key.lower().endswith(".csv"):
            return pd.read_csv(io.BytesIO(body), **pd_kwargs)

        elif s3_key.lower().endswith(".json"):
            return pd.read_json(io.BytesIO(body), orient="columns", **pd_kwargs)

        else:
            raise ValueError(f"Unsupported file type in key: {s3_key}")
        
    except ClientError as e:
        raise


def upload_df_as_parquet_to_s3(df: pd.DataFrame, s3_target_key: str):
    """Write a pandas DataFrame to S3 as Parquet without writing locally."""
    # Convert DataFrame to Parquet bytes in memory
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Upload to S3
    s3 = s3_dest
    s3.put_object(
        Bucket=S3_TARGET_BUCKET,
        Key=s3_target_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )
    return f"s3://{S3_TARGET_BUCKET}/{s3_target_key}"


def generate_s3_partitioned_key(prefix: str, original_key: str) -> str:
    """
    Generate a partitioned S3 key based on the date inside the filename.
    Expects a filename formatted like: call_logs_day_2025-11-20.csv
    """
    filename = original_key.split("/")[-1]               # call_logs_day_2025-11-20.csv
    date_str = filename.replace(".csv", "").split("_")[-1]  # 2025-11-20
    year, month, day = date_str.split("-")
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parquet_name = f"{prefix}{ts}.parquet"
    return f"{prefix}/{year}/{month}/{parquet_name}"