with messages as (

    select * from {{ ref('stg_telegram_messages') }}

),

channels as (

    select channel_key, channel_name from {{ ref('dim_channels') }}

)

select
    md5(c.channel_key || m.message_id::text) as message_key,
    m.message_id,
    c.channel_key,
    to_char(m.message_date, 'YYYYMMDD')::int as date_key,
    m.message_text,
    m.message_length,
    m.views,
    m.forwards,
    m.has_image
from messages m
left join channels c on m.channel_name = c.channel_name
