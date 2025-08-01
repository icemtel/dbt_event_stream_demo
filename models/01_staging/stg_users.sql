select
    user_id,
    first_name,
    last_name,
    first_name || ' '  || last_name as full_name,
    created_at,
    updated_at,
    deleted_at,
    country_code,
    favorite_color as fav_color

from {{ source('raw', 'user') }}