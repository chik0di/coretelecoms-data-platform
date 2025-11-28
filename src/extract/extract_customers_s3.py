import datetime
import logging
from src.utils.config import S3_TARGET_BUCKET
from src.utils.s3_utils import read_csv_from_s3_to_df, upload_df_as_parquet_to_s3
from src.utils.pre_bronze import normalize_column_name_and_type, add_ingest_metadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run(s3_key: str, write_back_to_s3: bool = False):
    logger.info("Starting customers extraction: %s", s3_key)
    df = read_csv_from_s3_to_df(s3_key)

    df = normalize_column_name_and_type(df)
    df = add_ingest_metadata(df, source=s3_key)

    if write_back_to_s3:
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        parquet_name = f"customers_{timestamp}.parquet"
        s3_target_key = f"customers/{parquet_name}"
        upload_df_as_parquet_to_s3(df, s3_target_key)
        logger.info("Uploaded parquet to s3://%s/%s", S3_TARGET_BUCKET, s3_target_key)

if __name__ == "__main__":
    run("customers/customers_dataset.csv", write_back_to_s3=True)
