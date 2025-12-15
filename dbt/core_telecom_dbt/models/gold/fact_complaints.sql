{{ config(materialized='table') }}

WITH unified_complaints AS (

    SELECT
        "call_id" AS "complaint_id",
        "customer_id",
        "agent_id",
        'call_log' AS "channel",
        "complaint_category",
        "call_start_time" AS "request_timestamp",
        "is_resolved",
        "resolution_status",
        "call_end_time" AS "resolution_timestamp",
        CASE 
            WHEN "resolution_status" = 'Resolved'
            THEN "call_duration_seconds" / 3600.0
            ELSE NULL
        END AS "resolution_time_hours"
    FROM {{ ref('call_log') }}

    UNION ALL

    SELECT
        "request_id" AS "complaint_id",
        "customer_id",
        "agent_id",
        'webform' AS "channel",
        "complaint_category",
        "request_date" AS "request_timestamp",
        "is_resolved",
        "resolution_status",
        "resolution_date" AS "resolution_timestamp",
        "resolution_time_hours"
    FROM {{ ref('webform') }}

    UNION ALL

    SELECT
        "complaint_id",
        "customer_id",
        "agent_id",
        "media_channel" AS "channel",
        "complaint_category",
        "request_date" AS "request_timestamp",
        "is_resolved",
        "resolution_status",
        "resolution_date" AS "resolution_timestamp",
        "resolution_time_hours"
    FROM {{ ref('social_media') }}
),

fact_with_keys AS (
    SELECT
        f."complaint_id",
        f."customer_id",
        f."agent_id",
        d."category_id" AS "complaint_category_id",
        f."channel",
        f."request_timestamp",
        f."resolution_timestamp",
        f."resolution_time_hours",
        f."is_resolved",
        f."resolution_status"
    FROM unified_complaints f
    LEFT JOIN {{ ref('dim_complaint_category') }} d
        ON f."complaint_category" = d."category_name"
)

SELECT * FROM fact_with_keys
