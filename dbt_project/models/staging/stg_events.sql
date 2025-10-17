-- Staging model for events - cleans and validates raw event data
-- This model implements incremental processing for cost optimization

{{ config(
    materialized='incremental',
    partition_by={
        "field": "event_date",
        "data_type": "date"
    },
    cluster_by=["user_id", "event_type", "platform"],
    unique_key="event_id",
    on_schema_change="sync_all_columns"
) }}

WITH raw_events AS (
    SELECT
        event_id,
        user_id,
        event_type,
        event_timestamp,
        session_id,
        device_type,
        platform,
        app_version,
        metadata,
        created_at,
        event_date
    FROM {{ source('raw', 'events') }}
    {% if is_incremental() %}
        -- Only process new data for incremental runs
        WHERE event_date >= (SELECT MAX(event_date) FROM {{ this }})
    {% endif %}
),

-- Data validation and cleaning
validated_events AS (
    SELECT
        event_id,
        user_id,
        event_type,
        event_timestamp,
        session_id,
        device_type,
        platform,
        app_version,
        metadata,
        created_at,
        event_date,
        
        -- Validation flags
        CASE 
            WHEN event_id IS NULL OR event_id = '' THEN FALSE
            WHEN user_id IS NULL OR user_id = '' THEN FALSE
            WHEN event_type IS NULL OR event_type = '' THEN FALSE
            WHEN event_timestamp IS NULL THEN FALSE
            ELSE TRUE
        END as is_valid,
        
        -- Collect validation errors
        ARRAY(
            SELECT error
            FROM UNNEST([
                CASE WHEN event_id IS NULL OR event_id = '' THEN 'missing_event_id' END,
                CASE WHEN user_id IS NULL OR user_id = '' THEN 'missing_user_id' END,
                CASE WHEN event_type IS NULL OR event_type = '' THEN 'missing_event_type' END,
                CASE WHEN event_timestamp IS NULL THEN 'missing_event_timestamp' END
            ]) as error
            WHERE error IS NOT NULL
        ) as validation_errors,
        
        -- Parsed metadata fields
        CASE 
            WHEN JSON_VALUE(metadata, '$.amount') IS NOT NULL 
            THEN CAST(JSON_VALUE(metadata, '$.amount') AS FLOAT64)
            ELSE NULL
        END as amount,
        
        JSON_VALUE(metadata, '$.currency') as currency,
        JSON_VALUE(metadata, '$.product_id') as product_id,
        JSON_VALUE(metadata, '$.category') as category,
        JSON_VALUE(metadata, '$.page') as page,
        JSON_VALUE(metadata, '$.source') as source,
        JSON_VALUE(metadata, '$.campaign') as campaign,
        
        -- Data quality metrics
        LENGTH(COALESCE(event_id, '')) as event_id_length,
        LENGTH(COALESCE(user_id, '')) as user_id_length,
        LENGTH(COALESCE(event_type, '')) as event_type_length
        
    FROM raw_events
)

SELECT
    event_id,
    user_id,
    event_type,
    event_timestamp,
    session_id,
    device_type,
    platform,
    app_version,
    metadata,
    created_at,
    event_date,
    is_valid,
    validation_errors,
    amount,
    currency,
    product_id,
    category,
    page,
    source,
    campaign,
    event_id_length,
    user_id_length,
    event_type_length,
    CURRENT_TIMESTAMP() as processed_at
FROM validated_events


