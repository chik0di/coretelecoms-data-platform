{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['"customer_id"']) }} AS "customer_key",
    "customer_id",
    "name" AS "customer_name",
    "gender",
    "date_of_birth",
    "signup_date",
    "email_address",
    "address",
    "state"
FROM {{ ref('customer') }}
WHERE "customer_id" IS NOT NULL
