with
    u as (select * from {{ ref('stg_users') }}),
    c as (select * from {{ ref('countries') }}),

    tmp as (
        select
            u.*,
            c.name as country,
            c.region
        from u
        left join c using (country_code)
    )

-- organize columns
select
    user_id,
    first_name,
    last_name,
    full_name,

    -- cohort attributes
    country_code,
    country,
    region,
    fav_color,

    -- timestamps
    created_at,
    updated_at,
    deleted_at
from tmp
