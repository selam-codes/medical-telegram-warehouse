with bounds as (

    select
        min(message_date)::date as start_date,
        max(message_date)::date as end_date
    from {{ ref('stg_telegram_messages') }}

),

spine as (

    select generate_series(start_date, end_date, interval '1 day')::date as full_date
    from bounds

)

select
    to_char(full_date, 'YYYYMMDD')::int        as date_key,
    full_date,
    extract(dow from full_date)::int           as day_of_week,
    trim(to_char(full_date, 'Day'))             as day_name,
    extract(week from full_date)::int          as week_of_year,
    extract(month from full_date)::int         as month,
    trim(to_char(full_date, 'Month'))           as month_name,
    extract(quarter from full_date)::int       as quarter,
    extract(year from full_date)::int          as year,
    (extract(dow from full_date) in (0, 6))    as is_weekend
from spine
