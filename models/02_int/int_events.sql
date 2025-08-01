{{ config(
    materialized='ephemeral'
) }}

SELECT *
from {{ ref('stg_events') }}
