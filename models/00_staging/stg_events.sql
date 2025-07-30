{#
Incremental strategy:
in production, switch to `microbatch` strategy:
- eliminates is_incremental() logic altogether
- supposedly efficient on large data volumes

Since we're on DuckDB, we have to filter the data to ingest ourselves:
To find the latest created_at, we want to run something like `SELECT  MAX(created_at)`,
but this would scan all the partitions.
To avoid that,
1. Find the latest partition (partitioned by date)
2. Find the latest timestamp in that partition
#}
{{ config(
    materialized         = 'incremental',
    unique_key           = 'event_id',
    incremental_strategy = 'delete+insert',
    partition_by         = { "field": "partition_date", "data_type": "date" }
) }}

{% if is_incremental() %}
WITH
  latest_partition AS (
    SELECT MAX(partition_date) AS last_day
    FROM {{ this }}
  ),

  max_table AS (
    SELECT MAX(created_at) AS max_value
    FROM {{ this }}
    WHERE partition_date = (SELECT last_day FROM latest_partition)
  )
  {% endif %}

SELECT
    event_id,
    user_id,
    post_id,
    created_at,
    event_type,
    CASE WHEN event_type = 'like' THEN 1 ELSE 0 END AS is_like,
    DATE(created_at) AS partition_date
FROM {{ source('raw','event') }}

{% if is_incremental() %}
WHERE created_at > (SELECT max_value FROM max_table)
{% endif %}

