import logging
from src.utils.config import S3_TARGET_BUCKET
from src.utils.s3_utils import upload_df_as_parquet_to_s3, get_target_s3, read_csv_from_s3_in_chunks
from src.utils.pre_bronze import normalize_column_name_and_type, add_ingest_metadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run(s3_key: str, write_back_to_s3: bool = False):
    logger.info("Starting customers extraction: %s", s3_key)
    chunk_iter = read_csv_from_s3_in_chunks(s3_key, chunksize=50000)

    for chunk in chunk_iter:
        chunk = normalize_column_name_and_type(chunk)
        chunk = add_ingest_metadata(chunk, source=s3_key)

        if write_back_to_s3:
            parquet_name = "core-telecom-customer-data.parquet"
            s3_target_key = f"customers/{parquet_name}"
            target_hook = get_target_s3()
            
            if target_hook.check_for_key(s3_target_key, bucket_name=S3_TARGET_BUCKET):
                logger.info("Skipping upload — parquet already exists: %s", s3_target_key)
                return
            upload_df_as_parquet_to_s3(chunk, s3_target_key)
            logger.info("Uploaded parquet to s3://%s/%s", S3_TARGET_BUCKET, s3_target_key)
            