{{ config(materialized="table", tags=["dimension"]) }} 

with
    numbers as (
        -- Generate a sequence of numbers (days)
        select row_number() over () - 1 as day_number
        from unnest(generate_array(0, 3650)) as x  -- 10 years; adjust as needed
    )

select
    cast(
        format_date(
            '%Y%m%d', date_add(date('2024-01-01'), interval day_number day)
        ) as int64
    ) as dim_date_key,
    date_add(date('2024-01-01'), interval day_number day) as calendar_date,
    extract(
        year from date_add(date('2024-01-01'), interval day_number day)
    ) as calendar_year,
    extract(
        quarter from date_add(date('2024-01-01'), interval day_number day)
    ) as calendar_quarter,
    extract(
        month from date_add(date('2024-01-01'), interval day_number day)
    ) as calendar_month,
    format_date(
        '%A', date_add(date('2024-01-01'), interval day_number day)
    ) as calendar_day_name,
    format_date(
        '%B', date_add(date('2024-01-01'), interval day_number day)
    ) as calendar_month_name,
    case
        when
            extract(
                dayofweek from date_add(date('2024-01-01'), interval day_number day)
            )
            in (1, 7)
        then true
        else false
    end as is_weekend
from numbers
where date_add(date('2024-01-01'), interval day_number day) <= current_date