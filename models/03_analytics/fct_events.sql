{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='fail'
) }}

SELECT *
from {{ ref('int_events') }}
