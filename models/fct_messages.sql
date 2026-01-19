{{ config(materialized='table') }}

SELECT 
    m.id as message_id,
    c.channel_key,
    m.message_text,
    m.message_date
FROM {{ ref('stg_messages') }} m
JOIN {{ ref('dim_channels') }} c ON m.channel_username = c.channel_username