with cte_date_base as 
(
select distinct calendar_date
from {{ ref('stage_pro_wrestling_events') }}
where calendar_date IS NOT NULL
)

select
  cast(format_date('%Y%m%d', calendar_date) as int64) as dim_date_key  -- yyyymmdd as integer
  , calendar_date
  , extract(year from calendar_date) as calendar_year
  , (format_date('%A', calendar_date)) as day_name                  -- e.g., 'monday'
  , (format_date('%B', calendar_date)) as month_name                -- e.g., 'january'
  , extract(quarter from calendar_date) as calendar_quarter
  , extract(week from calendar_date) as week_of_year
  , extract(dayofyear from calendar_date) as day_of_year
  , case
      when extract(dayofweek from calendar_date) in (1, 7) then true
      else false
    end as is_weekend
from cte_date_base
