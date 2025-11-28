import logging
from src.utils.config import (
    S3_RAW_BUCKET,
    S3_TARGET_BUCKET
)
from src.utils.s3_utils import read_csv_from_s3_to_df, upload_df_as_parquet_to_s3, generate_s3_partitioned_key, s3_source
from src.utils.pre_bronze import normalize_column_name_and_type, add_ingest_metadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(s3_key: str, write_back_to_s3: bool = False):
    logger.info("Starting social media data extraction: %s", s3_key)
    df = read_csv_from_s3_to_df(s3_key)

    df = normalize_column_name_and_type(df)
    df = add_ingest_metadata(df, source=s3_key)

    if write_back_to_s3: 
        s3_target_key = generate_s3_partitioned_key("social-media", s3_key)
        upload_df_as_parquet_to_s3(df, s3_target_key)
        logger.info("Uploaded parquet to s3://%s/%s", S3_TARGET_BUCKET, s3_target_key)


def run_folder(prefix: str):
    s3=s3_source
    response = s3.list_objects_v2(Bucket=S3_RAW_BUCKET, Prefix=prefix)

    if "Contents" not in response:
        logger.info("No files found under prefix: %s", prefix)
        return

    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith(".json"):
            logger.info("Processing json: %s", key)
            run(key, write_back_to_s3=True)

if __name__ == "__main__":
    run_folder("social_medias/")