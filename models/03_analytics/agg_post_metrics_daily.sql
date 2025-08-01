{{ config(
    materialized='incremental',
    unique_key=['event_date', 'post_id'],
    incremental_strategy='delete+insert',
    partition_by={ "field": "event_date", "data_type": "date" }
) }}

{% if is_incremental() %}
    with
        latest_partition as (select max(event_date) as last_day from {{ this }})
{% endif %}

select
    partition_date as event_date,
    post_id,
    sum(is_like) as likes,
    count(distinct case when not is_like then user_id end) as views
from {{ ref('int_events') }}
{% if is_incremental() %}
    where
        partition_date >= (select last_day from latest_partition)
{% endif %}
group by partition_date, post_id