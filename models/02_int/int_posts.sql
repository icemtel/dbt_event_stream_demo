with
    p as (select * from {{ ref('stg_posts') }}),
    u as (select * from {{ ref('int_users') }}),

    tmp as (
        select
            p.* exclude (user_id),
            -- post features
            -- TODO: Add derived fields like post_length
            -- creator info
            p.user_id as creator_id,
            u.country_code as creator_country_code,
            u.country as creator_country,
            u.region as creator_region,
            u.fav_color as creator_fav_color,

        from p
        left join u using (user_id)
    )

select
    post_id,
    post_text,

    -- creator info
    creator_country_code,
    creator_country,
    creator_region,
    creator_fav_color,

    -- timestamps
    created_at,
    updated_at,
    deleted_at,
    -- related ids
    creator_id,
from tmp
