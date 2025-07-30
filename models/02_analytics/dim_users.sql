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
    deleted_at, -- TODO: filter deleted users
from {{ ref('int_users') }}
