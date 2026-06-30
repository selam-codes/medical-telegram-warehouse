with detections as (

    select
        channel_name,
        message_id::bigint              as message_id,
        detected_class,
        confidence_score,
        image_category
    from {{ source('raw', 'yolo_detections') }}

),

channels as (

    select channel_key, channel_name from {{ ref('dim_channels') }}

),

messages as (

    select message_key, message_id, channel_key, date_key
    from {{ ref('fct_messages') }}

)

select
    d.channel_name,
    c.channel_key,
    m.date_key,
    d.message_id,
    m.message_key,
    d.detected_class,
    d.confidence_score,
    d.image_category
from detections d
left join channels c on d.channel_name = c.channel_name
left join messages m on d.message_id = m.message_id and c.channel_key = m.channel_key
