with messages as (

    select * from {{ ref('stg_telegram_messages') }}

),

agg as (

    select
        channel_name,
        min(message_date) as first_post_date,
        max(message_date) as last_post_date,
        count(*)           as total_posts,
        avg(views)          as avg_views
    from messages
    group by channel_name

)

select
    md5(channel_name) as channel_key,
    channel_name,
    case
        when lower(channel_name) like '%pharma%'    then 'Pharmaceutical'
        when lower(channel_name) like '%cosmetic%'  then 'Cosmetics'
        else 'Medical'
    end as channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    avg_views
from agg
