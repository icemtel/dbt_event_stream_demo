with
u as
(select
    *
from {{ ref('int_users') }}

),
user_totals
as
(
SELECT user_id, sum(likes_given) as likes_given, sum(posts_viewed) as posts_viewed
from {{ref('agg_user_metrics_daily')}}
group by user_id
)
,
creator_totals  as
 (
select creator_id as user_id, sum(likes) as total_likes, sum(views) as total_views
 from
 {{ref('dim_posts')}}
 group by creator_id
 )

SELECT u.* exclude(deleted_at),
       -- metrics
       likes_given,
       posts_viewed,
        -- creator metrics
        total_likes,
        total_views
FROM u
LEFT JOIN user_totals using (user_id)
LEFT JOIN creator_totals using (user_id)
where deleted_at is null
-- Soft-deletes are removed to simplify analyst queries
-- Not removed earlier for a proper join with posts in the intermediate layer
-- (posts are kept on the platform even if the creator is deleted unless they request that)