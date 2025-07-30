select
    post_id,
    user_id,
    post_text,
    created_at,
    updated_at,
    deleted_at,

from {{ source('raw', 'post') }}
