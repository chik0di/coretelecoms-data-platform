import logging
from src.utils.config import (
    S3_RAW_BUCKET,
    S3_TARGET_BUCKET
)
from src.utils.s3_utils import read_csv_from_s3_to_df, upload_df_as_parquet_to_s3, generate_s3_partitioned_key, get_source_s3, get_target_s3
from src.utils.pre_bronze import normalize_column_name_and_type, add_ingest_metadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run(s3_key: str, write_back_to_s3: bool = False):
    logger.info("Starting social media data extraction: %s", s3_key)

    if write_back_to_s3: 
        s3_target_key = generate_s3_partitioned_key("social-media", s3_key)
        target_hook = get_target_s3()
        
        if target_hook.check_for_key(s3_target_key, bucket_name=S3_TARGET_BUCKET):
            logger.info("Skipping upload — parquet already exists: %s", s3_target_key)
            return
        
    df = read_csv_from_s3_to_df(s3_key)
    df = normalize_column_name_and_type(df)
    df = add_ingest_metadata(df, source=s3_key)
    
    if write_back_to_s3:
        upload_df_as_parquet_to_s3(df, s3_target_key)
        logger.info("Uploaded parquet to s3://%s/%s", S3_TARGET_BUCKET, s3_target_key)


def run_folder(prefix: str):
    s3_hook = get_source_s3()
    keys = s3_hook.list_keys(bucket_name=S3_RAW_BUCKET, prefix=prefix)

    if not keys:
        logger.info("No files found under prefix: %s", prefix)
        return

    for key in keys:
        if key.lower().endswith(".csv"):
            logger.info("Processing CSV: %s", key)
            run(key, write_back_to_s3=True)

if __name__ == "__main__":
    run_folder("call logs/")