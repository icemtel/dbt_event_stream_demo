select
    post_id,
    post_text,

    -- author info
    author_country_code,
    author_country,
    author_region,
    author_fav_color,

    -- timestamps
    created_at,
    updated_at,

    -- related ids
    author_user_id,

from {{ ref('int_posts') }}
where deleted_at is null -- see comment in dim_users.sql