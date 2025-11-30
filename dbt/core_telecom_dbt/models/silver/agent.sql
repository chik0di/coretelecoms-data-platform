{{ config(
    materialized = 'view'
) }}

WITH staged_agents AS (
    SELECT
        "id" AS "agent_id",
        "name" AS "full_name",
        "experience",
        "state",
        current_timestamp() AS "ingestion_time"

    FROM {{ source('staging', 'agent') }}
)

SELECT * FROM staged_agents