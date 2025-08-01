{{ config(
    materialized='view'
) }}

SELECT *
from {{ ref('int_events') }}
