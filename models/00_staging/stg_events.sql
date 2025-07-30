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
    with
        latest_partition as (select max(partition_date) as last_day from {{ this }}),

        max_table as (
            select max(created_at) as max_value
            from {{ this }}
            where partition_date = (select last_day from latest_partition)
        )
{% endif %}

select
    event_id,
    user_id,
    post_id,
    created_at,
    event_type,
    case when event_type = 'like' then 1 else 0 end as is_like,
    date(created_at) as partition_date
from {{ source('raw','event') }}

{% if is_incremental() %} where created_at > (select max_value from max_table) {% endif %}
