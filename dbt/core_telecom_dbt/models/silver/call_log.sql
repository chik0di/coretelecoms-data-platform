{{ config(
    materialized = 'view'
) }}

WITH staged_call_logs AS (
    SELECT
        "call_id",
        "customer_id",
        "complaint_catego_ry" AS "complaint_category",
        "agent_id",
        "call_start_time"::DATETIME AS "call_start_time",
        "call_end_time"::DATETIME AS "call_end_time",
        "resolutionstatus" AS "resolution_status",
        "calllogsgenerationdate"::DATE AS "call_logs_generation_date",
        current_timestamp() AS "ingestion_time"

    FROM {{ source('staging', 'call_log') }}
    WHERE "call_id" IS NOT NULL
)

SELECT 
    "call_id",
    "customer_id",
    "complaint_category",
    "agent_id",
    "call_start_time",
    "call_end_time",
    TIMESTAMPDIFF(SECOND, "call_start_time", "call_end_time") AS "call_duration_seconds",
    "resolution_status",
    CASE 
        WHEN "resolution_status" = 'Resolved' THEN 1 
        ELSE 0
    END AS "is_resolved",
    "call_logs_generation_date",
    "ingestion_time"
FROM staged_call_logs