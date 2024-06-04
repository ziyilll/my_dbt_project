-- models/traffic.sql

{{ config(
    materialized='table'
) }}

SELECT
    session_id,
    user_id,
    video_id,
    view_duration,
    interaction_type,
    product_id
FROM
    hive_database_name.traffic
