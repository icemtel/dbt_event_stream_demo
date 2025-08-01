with p
as
(
select *
from {{ ref('int_posts') }}
),
post_totals
as
(
SELECT post_id, sum(likes) as likes, sum(views) as views
from {{ref('agg_post_metrics_daily')}}
group by post_id
)


SELECT p.* exclude(deleted_at),
        -- metrics
        likes,
        views
FROM p
LEFT join post_totals using (post_id)


where deleted_at is null -- see comment in dim_users.sql
