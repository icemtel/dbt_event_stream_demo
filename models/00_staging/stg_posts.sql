select
    post_id,
    user_id,
    post_text,
    created_at,
    updated_at,
    deleted_at,
    -- TODO: Add derived fields like post_length, word_count

from {{ source('raw', 'post') }}
