select
    p.*,
    p.user_id as author_user_id,
    u.country_code as author_country_code,
from {{ ref('stg_posts') }} p
left join {{ ref('stg_users') }} u on p.user_id = u.user_id