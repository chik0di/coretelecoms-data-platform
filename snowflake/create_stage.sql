USE DATABASE CORE_TELECOM_DB;

CREATE OR REPLACE STAGE s3_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://core-telecoms-dev-bronze-layer/';

LIST @s3_stage;