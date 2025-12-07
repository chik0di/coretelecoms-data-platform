import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.config import Config
from .config import S3_RAW_BUCKET, S3_TARGET_BUCKET

def get_source_s3():
    """Get S3 hook for source bucket with timeout configuration"""
    config = Config(
        read_timeout=600,      # 10 minutes for reading large files
        connect_timeout=60,    # 1 minute to establish connection
        retries={
            'max_attempts': 3,
            'mode': 'standard'
        }
    )
    
    return S3Hook(
        aws_conn_id="aws_source_bucket_conn",
        config=config
    )

def get_target_s3():
    """Get S3 hook for target bucket with optimized upload settings"""
    s3_hook = S3Hook(
        aws_conn_id='aws_destination_conn',
        transfer_config_args={
            'max_concurrency': 5,                    # Reduced for stability
            'multipart_threshold': 5 * 1024 * 1024,  # 5MB
            'multipart_chunksize': 5 * 1024 * 1024,  # 5MB chunks
        }
    )
    return s3_hook

def list_objects(prefix: str = ""):
    """List objects in source S3 bucket"""
    prefix = prefix.lstrip("/")
    s3_hook = get_source_s3()
    
    keys = s3_hook.list_keys(bucket_name=S3_RAW_BUCKET, prefix=prefix)
    return keys or []

def read_csv_from_s3_to_df(s3_key: str, **pd_kwargs) -> pd.DataFrame:
    """
    Read a CSV or JSON file from S3 and return it as a pandas DataFrame
    with proper timeout handling for large files
    """
    s3_hook = get_source_s3()  # Now includes timeout config
    s3_obj = s3_hook.get_key(key=s3_key, bucket_name=S3_RAW_BUCKET)
    body = s3_obj.get()["Body"].read()
    
    if s3_key.lower().endswith(".csv"):
        return pd.read_csv(io.BytesIO(body), **pd_kwargs)
    elif s3_key.lower().endswith(".json"):
        return pd.read_json(io.BytesIO(body), orient="columns", **pd_kwargs)
    else:
        raise ValueError(f"Unsupported file type in key: {s3_key}")
    

def read_csv_from_s3_in_chunks(s3_key: str, chunksize: int = 50000, **pd_kwargs):
    """
    Stream a CSV file from S3 in chunks without loading entire file into memory.
    Returns a generator yielding pandas DataFrame chunks.
    """
    s3_hook = get_source_s3()
    s3_obj = s3_hook.get_key(key=s3_key, bucket_name=S3_RAW_BUCKET)

    # STREAM instead of reading full object
    body_stream = s3_obj.get()["Body"]

    # pandas can read directly from a file-like stream
    return pd.read_csv(body_stream, chunksize=chunksize, **pd_kwargs)

def upload_df_as_parquet_to_s3(df: pd.DataFrame, s3_target_key: str):
    """Write a pandas DataFrame to S3 as Parquet with timeout handling"""
    # Convert DataFrame to Parquet bytes in memory
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Upload to S3 with optimized settings
    s3_hook = get_target_s3()  # Includes multipart upload config
    s3_hook.load_bytes(
        bytes_data=buffer.getvalue(),
        key=s3_target_key,
        bucket_name=S3_TARGET_BUCKET,
        replace=True
    )
    return f"s3://{S3_TARGET_BUCKET}/{s3_target_key}"

def generate_s3_partitioned_key(prefix: str, original_key: str) -> str:
    """
    Generate a partitioned S3 key based on the date inside the filename.
    Expects a filename formatted like: call_logs_day_2025-11-20.csv
    """
    filename = original_key.split("/")[-1]
    date_str = filename.replace(".csv", "").replace(".json", "").split("_")[-1]
    parts = date_str.split("-")
    
    if len(parts) != 3:
        raise ValueError(
            f"Invalid date format in filename: {filename}. "
            f"Expected format: prefix_YYYY-MM-DD.ext, got date_str: '{date_str}'"
        )
    
    year, month, day = parts
    parquet_name = f"{prefix}-{year}-{month}-{day}.parquet"
    return f"{prefix}/{year}/{month}/{parquet_name}"