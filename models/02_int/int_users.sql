with
u as ( select * from {{ ref('stg_users') }} ),
c as ( select * from {{ref('countries') }} )


select u.*,
       c.name as country,
       c.region
from u
left join c using (country_code)