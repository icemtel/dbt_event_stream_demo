with
    base as (
        {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2100-01-01' as date)",
    end_date="cast('2101-01-01' as date)"
   )
}}
    )

select
    date_day,

    date_trunc('month', date_day) as date_month,
    date_trunc('quarter', date_day) as date_quarter,
    date_trunc('year', date_day) as date_year,

    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    strftime(date_day, '%B') as month_name,
    extract(day from date_day) as day_of_month,
    cast(strftime(date_day, '%j') as int) as day_of_year,

    case
        when cast(strftime(date_day, '%w') as int) = 0 then 7 else cast(strftime(date_day, '%w') as int)
    end as iso_day_of_week,
    (cast(strftime(date_day, '%w') as int) in (0, 6)) as is_weekend,
from base
