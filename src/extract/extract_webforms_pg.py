from sqlalchemy import create_engine, text
import pandas as pd 
import logging
from src.utils.config import (
    S3_TARGET_BUCKET
)
from src.utils.s3_utils import get_target_s3, upload_df_as_parquet_to_s3
from src.utils.postgres_utils import list_webform_tables, generate_s3_partitioned_key, get_postgres_engine, get_db_credentials
from src.utils.pre_bronze import normalize_column_name_and_type, add_ingest_metadata


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_and_upload_forms(write_back_to_s3: bool = False):
    engine = get_postgres_engine()
    tables = list_webform_tables()
    schema = get_db_credentials(return_only="schema")
    database = get_db_credentials(return_only="database")

    for table in tables:

        # year, month, day = table.split("_")[-3], table.split("_")[-2], table.split("_")[-1]

        # # Check if already ingested to S3
        # s3_prefix = f"website-complaint-forms/{year}/{month}/"
        # if s3_partition_exists(S3_TARGET_BUCKET, s3_prefix):
        #     logger.info("Skipping %s — already exists in S3.", table)
        #     continue

        logger.info("Extracting new table: %s", table)
        
        query = text(f'SELECT * FROM {schema}.{table}')
        with engine.connect() as conn:
            logger.info("Starting website complaint form extraction: %s", table)
            df = pd.read_sql(query, conn)

            # Pre-bronze processing
            df = normalize_column_name_and_type(df)
            df = add_ingest_metadata(df, source=database)

            # upload to S3 as Parquet
            if write_back_to_s3: 
                s3_target_key = generate_s3_partitioned_key("website-complaint-forms", table)
                target_hook = get_target_s3()
        
                if target_hook.check_for_key(s3_target_key, bucket_name=S3_TARGET_BUCKET):
                    logger.info("Skipping upload — parquet already exists: %s", s3_target_key)
                    return
            upload_df_as_parquet_to_s3(df, s3_target_key)
            logger.info("Uploaded parquet to s3://%s/%s", S3_TARGET_BUCKET, s3_target_key)