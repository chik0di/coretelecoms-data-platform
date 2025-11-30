{{ config(
    materialized = 'view'
) }}

WITH staged_social_media AS (
    SELECT  
        "complaint_id",
        "customer_id",
        "complaint_catego_ry" AS "complaint_category",
        "agent_id",
        "resolutionstatus" AS "resolution_status",

        TRY_TO_TIMESTAMP("request_date") AS "request_date",
        TRY_TO_TIMESTAMP("resolution_date") AS "resolution_date",

        "media_channel" AS "media_channel",
        TRY_TO_DATE("mediacomplaintgenerationdate") AS "media_complaint_generation_date",

        CURRENT_TIMESTAMP() AS "_ingest_time",
        'staging.social_media' AS "_source"
    
    FROM {{ source('staging', 'social_media') }}
    WHERE "complaint_id" IS NOT NULL
)

SELECT 
    "complaint_id",
    "customer_id",
    "complaint_category",
    "agent_id",
    "resolution_status",
    "request_date",
    "resolution_date",
    DATEDIFF(
        hour,
        "request_date",
        "resolution_date"
    ) AS "resolution_time_hours",
    CASE 
        WHEN "resolution_status" = 'Resolved' THEN 1 
        ELSE 0
    END AS "is_resolved",
    "media_channel",
    "media_complaint_generation_date",
    "_ingest_time",
    "_source"
FROM staged_social_media
