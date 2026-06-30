with source as (

    select * from {{ source('raw', 'telegram_messages') }}

),

cleaned as (

    select
        message_id::bigint                         as message_id,
        trim(channel_name)                          as channel_name,
        message_date::timestamptz                   as message_date,
        message_text                                as message_text,
        coalesce(has_media, false)                  as has_media,
        image_path                                  as image_path,
        coalesce(views, 0)::int                      as views,
        coalesce(forwards, 0)::int                   as forwards,
        length(coalesce(message_text, ''))          as message_length,
        (image_path is not null)                    as has_image
    from source
    -- drop empty/invalid records: a message must have either text or media to be analytically useful
    where message_date is not null
      and (
        (message_text is not null and trim(message_text) != '')
        or has_media = true
      )

),

deduped as (

    select distinct *
    from cleaned

)

select * from deduped
