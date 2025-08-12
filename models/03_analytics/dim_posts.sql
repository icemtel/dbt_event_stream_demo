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
        coalesce(likes, 0) as likes,
        coalesce(views, 0) as views
FROM p
LEFT join post_totals using (post_id)


where deleted_at is null