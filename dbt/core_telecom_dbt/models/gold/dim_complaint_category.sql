{{ config(materialized='table') }}

WITH raw_categories AS (

    SELECT "complaint_category" FROM {{ ref('call_log') }}
    UNION ALL
    SELECT "complaint_category" FROM {{ ref('social_media') }}
    UNION ALL
    SELECT "complaint_category" FROM {{ ref('webform') }}

),

cleaned AS (
    SELECT DISTINCT
        TRIM(LOWER("complaint_category")) AS "complaint_category"
    FROM raw_categories
    WHERE "complaint_category" IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY "complaint_category") AS "category_id",
    INITCAP("complaint_category") AS "category_name",
    NULL AS description
FROM cleaned
