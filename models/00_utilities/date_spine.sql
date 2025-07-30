{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2100-01-01' as date)",
    end_date="cast('2101-01-01' as date)"
   )
}}