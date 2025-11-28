from sqlalchemy import create_engine, text
import pandas as pd 
import logging
from src.utils.config import (
    S3_TARGET_BUCKET
)
from src.utils.s3_utils import upload_df_as_parquet_to_s3
from src.utils.postgres_utils import list_webform_tables, generate_s3_partitioned_key # s3_partition_exists
from src.utils.postgres_utils import (
    host,
    database,
    user,
    port,
    password,
    schema
)
from src.utils.pre_bronze import normalize_column_name_and_type, add_ingest_metadata


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_and_upload_forms(write_back_to_s3: bool = False):

    engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )
    tables = list_webform_tables()

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
                upload_df_as_parquet_to_s3(df, s3_target_key)
                logger.info("Uploaded parquet to s3://%s/%s", S3_TARGET_BUCKET, s3_target_key)
 

if __name__ == "__main__":
    extract_and_upload_forms(write_back_to_s3=True)