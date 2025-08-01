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
from {{ ref('int_users') }}
where deleted_at is null
-- Soft-deletes are removed to simplify analyst queries
-- Not removed earlier for a proper join with posts in the intermediate layer
-- (posts are kept on the platform even if the author is deleted unless they request that)
