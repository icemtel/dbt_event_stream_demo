select
    event_id,
    user_id,
    post_id,
    event_ts,
    event_type,
    -- Simple business metric flags
    case
        when event_type = 'like' then 1
        else 0
    end as is_like,

    case
        when event_type = 'share' then 1
        else 0
    end as is_share,

    case
        when event_type = 'comment' then 1
        else 0
    end as is_comment,
    date(event_ts) as partition_date,
from {{ source('raw', 'events') }}
-- where created_at > (select max(created_at) from {{ this }})
-- TODO - do incremental.. but then this table has to exist in the staging layer...  for {{this }} to work..
