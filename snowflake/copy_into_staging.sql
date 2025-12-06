USE DATABASE CORE_TELECOM_DB;

-- copy into webform
COPY INTO core_telecom_db.staging.webform
FROM @s3_stage/website-complaint-forms/2025/11/
FILE_FORMAT = (FORMAT_NAME = 'my_parquet_format')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- copy into customer
COPY INTO core_telecom_db.staging.customer
FROM @s3_stage/customers/
FILE_FORMAT = (FORMAT_NAME = 'my_parquet_format')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- copy into agent
COPY INTO core_telecom_db.staging.agent
FROM @s3_stage/agents/
FILE_FORMAT = (FORMAT_NAME = 'my_parquet_format')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- copy into call log 
COPY INTO core_telecom_db.staging.call_log
FROM @s3_stage/call-logs/
FILE_FORMAT = (FORMAT_NAME = 'my_parquet_format')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- copy into social media
COPY INTO core_telecom_db.staging.social_media
FROM @s3_stage/social-media/
FILE_FORMAT = (FORMAT_NAME = 'my_parquet_format')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;