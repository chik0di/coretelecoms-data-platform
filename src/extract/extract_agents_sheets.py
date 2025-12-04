import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from src.utils.config import SERVICE_ACCOUNT_FILE, SPREADSHEET_NAME
from src.utils.pre_bronze import normalize_column_name_and_type, add_ingest_metadata
from src.utils.s3_utils import upload_df_as_parquet_to_s3, S3_TARGET_BUCKET, get_target_s3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly"
]
creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)
def read_google_sheet_to_df(spreadsheet: str, worksheet_name: str = None) -> pd.DataFrame:
     client = gspread.authorize(creds)
     sheet = client.open(spreadsheet)
     worksheet = sheet.worksheet(worksheet_name)
     data = worksheet.get_all_records()
     df = pd.DataFrame(data)
     return df


def run(write_back_to_s3: bool = False):

    logger.info("Starting customer agents extraction: %s", SPREADSHEET_NAME)
    df = read_google_sheet_to_df(spreadsheet=SPREADSHEET_NAME, worksheet_name="agents")

    df = normalize_column_name_and_type(df)
    df = add_ingest_metadata(df, source="google_sheets")

    if write_back_to_s3:
        parquet_name = f"core-telecom-agent-data.parquet"
        s3_target_key = f"agents/{parquet_name}"
        target_hook = get_target_s3()
        
        if target_hook.check_for_key(s3_target_key, bucket_name=S3_TARGET_BUCKET):
            logger.info("Skipping upload — parquet already exists: %s", s3_target_key)
            return
        
        upload_df_as_parquet_to_s3(df, s3_target_key)
        logger.info("Uploaded parquet to s3://%s/%s", S3_TARGET_BUCKET, s3_target_key)

if __name__ == "__main__":
    run(write_back_to_s3=True)