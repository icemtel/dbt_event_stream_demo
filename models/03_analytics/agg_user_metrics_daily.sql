
{{ config(
    materialized='incremental',
    unique_key=['event_date', 'user_id'],
    incremental_strategy='delete+insert',
    partition_by={ "field": "event_date", "data_type": "date" }
) }}

{% if is_incremental() %}
    with
        latest_partition as (select max(event_date) as last_day from {{ this }})
{% endif %}

select
    partition_date as event_date,
    user_id,
    count(distinct case when is_like then post_id end) as likes_given,
    count(distinct case when not is_like then post_id end) as posts_viewed, -- unique users
from {{ ref('int_events') }}

{% if is_incremental() %}
    where
        partition_date >= (select last_day from latest_partition)
{% endif %}
group by partition_date, user_id
