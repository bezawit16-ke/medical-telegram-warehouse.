{{ config(materialized='table') }}

SELECT 
    ROW_NUMBER() OVER (ORDER BY channel_username) as channel_key,
    channel_username, 
    channel_title
FROM (
    SELECT DISTINCT channel_username, channel_title 
    FROM {{ ref('stg_messages') }}
) sub