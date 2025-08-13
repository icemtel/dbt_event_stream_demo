/*
MAU by user region and month, calculated based on raw events table.
(Alternatively, daily metrics table could be used)
*/
with
    events as (
        select *
        from {{ ref("fct_events") }}
        where extract(year from partition_date)  = 2100 -- filter partitions; featch only 1 year
    ),
    user_snapshot as (select * from {{ ref("dim_users_snapshots") }}),

    joined as (
        select
            events.*,
            user_snapshot.region
        from events
        left join
            user_snapshot
            on events.user_id = user_snapshot.user_id
            and events.created_at >= user_snapshot.dbt_valid_from
            and events.created_at < user_snapshot.dbt_valid_to
    )

select
    date_trunc('month', created_at) as activity_month,
    region,
    count(distinct user_id) as mau
from joined
group by activity_month, region
order by activity_month desc, mau desc