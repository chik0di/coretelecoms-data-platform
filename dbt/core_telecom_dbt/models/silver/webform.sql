{{ config(
    materialized = 'view'
) }}

WITH staged_webforms AS (
    SELECT  
        "request_id",
        TRY_TO_TIMESTAMP("request_date") AS "request_date",
        "customer_id",
        "complaint_catego_ry" AS "complaint_category",
        "agent_id",
        "resolutionstatus" AS "resolution_status",
        TRY_TO_TIMESTAMP("resolution_date") AS "resolution_date",
        "webformgenerationdate"::DATE AS "webform_generation_date",
        current_timestamp() AS "ingestion_time"
    
    FROM {{ source('staging', 'webform') }}
    WHERE "request_id" IS NOT NULL
)
SELECT 
    "request_id",
    "request_date",
    "customer_id",
    "complaint_category",
    "agent_id",
    "resolution_status",
    CASE 
        WHEN "resolution_status" = 'Resolved' THEN 1 
        ELSE 0
    END AS "is_resolved",
    "resolution_date",
    DATEDIFF(
        hour,
        "request_date",
        "resolution_date"
    ) AS "resolution_time_hours",
    "webform_generation_date",
    "ingestion_time"
 FROM staged_webforms