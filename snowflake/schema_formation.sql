CREATE FILE FORMAT my_parquet_format TYPE = PARQUET;

-- agents table schema 
CREATE OR REPLACE TABLE core_telecom_db.staging.agent
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@s3_stage/agents/core-telecom-agents.parquet',
          FILE_FORMAT=>'my_parquet_format'
        )
      ));

-- Call logs table schema 
CREATE OR REPLACE TABLE core_telecom_db.staging.call_log
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@s3_stage/call-logs/2025/11/call-logs20251127T145233Z.parquet',
          FILE_FORMAT=>'my_parquet_format'
        )
      ));

-- Customer table schema 
CREATE OR REPLACE TABLE core_telecom_db.staging.customer
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@s3_stage/customers/customers_20251127T030101Z.parquet',
          FILE_FORMAT=>'my_parquet_format'
        )
      ));


-- webform schema
CREATE OR REPLACE TABLE core_telecom_db.staging.webform
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@s3_stage/website-complaint-forms/2025/11/webform_20251127T152458Z.parquet',
          FILE_FORMAT=>'my_parquet_format'
        )
      ));

-- social media schema
CREATE OR REPLACE TABLE core_telecom_db.staging.social_media
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@s3_stage/website-complaint-forms/2025/11/webform_20251127T152458Z.parquet',
          FILE_FORMAT=>'my_parquet_format'
        )
      ));

