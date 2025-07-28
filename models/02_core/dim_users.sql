select
    user_id,
    first_name,
    last_name,
    created_at,
    country_code,
    
from {{ ref('int_users') }}
