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
    deleted_at,

    -- related ids
    author_user_id,


from {{ ref('int_posts') }}


/*
valid_from
valid_to
is_valid
SK -- dbt_utils
deleted_at
is_deleted
*/