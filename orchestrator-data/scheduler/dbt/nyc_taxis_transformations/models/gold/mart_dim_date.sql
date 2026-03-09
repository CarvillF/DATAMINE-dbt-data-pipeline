
with date_spine as (
    select
        generate_series(
            '2022-01-01'::date,
            '2025-12-31'::date,
            '1 day'::interval
        )::date as date_day
)

select
    to_char(date_day, 'yyyymmdd')::int as date_key,
    date_day as date,
    extract(year from date_day)::int as year,
    extract(month from date_day)::int as month,
    extract(day from date_day)::int as day,
    extract(isodow from date_day)::int as day_of_week
from date_spine