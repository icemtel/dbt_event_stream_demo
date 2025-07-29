select
    event_id,
    user_id,
    post_id,
    created_at,
    event_type,
    -- Simple business metric flags
    case
        when event_type = 'like' then 1
        else 0
    end as is_like,
    date(created_at) as partition_date,
from {{ source('raw', 'event') }}
-- where created_at > (select max(created_at) from {{ this }})
-- TODO - do incremental.. but then this table has to exist in the staging layer...  for {{this }} to work..
