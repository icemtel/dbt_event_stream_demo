select
    user_id,
    first_name,
    last_name,
    created_at,
    updated_at,
    deleted_at,
    country_code,

from {{ source('raw', 'users') }}
