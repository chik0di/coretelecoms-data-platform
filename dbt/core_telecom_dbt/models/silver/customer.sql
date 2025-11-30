{{ config(
    materialized = 'view'
) }}

WITH staged_customers AS (
    SELECT
        "customer_id",
        "name",
        "gender",
        "date_of_birth"::DATE AS "date_of_birth",
        "signup_date"::DATE AS "signup_date",
        LOWER(
                CASE 
                    WHEN "email" ILIKE '%@hotmai.com' THEN REPLACE("email", '@hotmai.com', '@hotmail.com')
                    WHEN "email" ILIKE '%@gmail%' THEN REGEXP_REPLACE("email", '@.*', '@gmail.com', 1, 1)
                    ELSE "email"
                END
            ) AS "email_address",
        "address",
        TRIM(SPLIT_PART(RIGHT("address", 8), ' ', 1)) AS "state_code",
        current_timestamp() AS "ingestion_time"
    FROM {{ source('staging', 'customer') }}
)

SELECT 
    "customer_id",
    "name",
    "gender",
    "date_of_birth",
    "signup_date",
    "email_address",
    "address",
    {{ get_state_name('"state_code"') }} AS "state",
    "ingestion_time"
FROM staged_customers