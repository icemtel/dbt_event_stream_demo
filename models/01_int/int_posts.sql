with
p as ( select * from  {{ ref('stg_posts') }} ),
u as ( select * from  {{ ref('int_users') }} )

select
    p.* exclude (user_id),
    -- author info
    p.user_id as author_user_id,
    u.country_code as author_country_code,
    u.country as author_country,
    u.region as author_region,
    u.fav_color as author_fav_color,

from p
left join u using (user_id)