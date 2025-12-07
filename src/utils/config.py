from pathlib import Path

# AWS / S3 settings 
AWS_REGION = "eu-north-1" 
S3_RAW_BUCKET = "core-telecoms-data-lake"
S3_TARGET_BUCKET = "core-telecoms-dev-bronze-layer"

# Google Sheets settings
SPREADSHEET_NAME = "CORETELECOMMS AGENTS"
SERVICE_ACCOUNT_FILE = Path(__file__).parent.parent.parent / "credentials" / "contactcenter-data-pipeline-b3ebd323d071.json"