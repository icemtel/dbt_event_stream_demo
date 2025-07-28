select *

from {{ ref('int_posts') }}


/*
valid_from
valid_to
is_valid
SK -- dbt_utils
deleted_at
is_deleted
*/