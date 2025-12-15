{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['"agent_id"']) }} AS "agent_key",
    "agent_id",
    "full_name" AS "agent_name",
    "experience",
    "state"
FROM {{ ref('agent') }}
WHERE "agent_id" IS NOT NULL
