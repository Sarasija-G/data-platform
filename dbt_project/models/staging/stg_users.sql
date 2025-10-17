-- Staging model for users - cleans and validates raw user data
-- This model implements incremental processing for cost optimization

{{ config(
    materialized='incremental',
    partition_by={
        "field": "user_date",
        "data_type": "date"
    },
    cluster_by=["country", "subscription_tier"],
    unique_key="user_id",
    on_schema_change="sync_all_columns"
) }}

WITH raw_users AS (
    SELECT
        user_id,
        email,
        created_at,
        updated_at,
        first_name,
        last_name,
        country,
        timezone,
        subscription_tier,
        metadata,
        user_date
    FROM {{ source('raw', 'users') }}
    {% if is_incremental() %}
        -- Only process new or updated users for incremental runs
        WHERE user_date >= (SELECT MAX(user_date) FROM {{ this }})
           OR updated_at > (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
),

-- Data validation and cleaning
validated_users AS (
    SELECT
        user_id,
        email,
        created_at,
        updated_at,
        first_name,
        last_name,
        country,
        timezone,
        subscription_tier,
        metadata,
        user_date,
        
        -- Validation flags
        CASE 
            WHEN user_id IS NULL OR user_id = '' THEN FALSE
            WHEN email IS NULL OR email = '' THEN FALSE
            WHEN created_at IS NULL THEN FALSE
            WHEN NOT REGEXP_CONTAINS(email, r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$') THEN FALSE
            ELSE TRUE
        END as is_valid,
        
        -- Collect validation errors
        ARRAY(
            SELECT error
            FROM UNNEST([
                CASE WHEN user_id IS NULL OR user_id = '' THEN 'missing_user_id' END,
                CASE WHEN email IS NULL OR email = '' THEN 'missing_email' END,
                CASE WHEN created_at IS NULL THEN 'missing_created_at' END,
                CASE WHEN NOT REGEXP_CONTAINS(email, r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$') THEN 'invalid_email_format' END
            ]) as error
            WHERE error IS NOT NULL
        ) as validation_errors,
        
        -- Parsed metadata fields
        JSON_VALUE(metadata, '$.source') as source,
        JSON_VALUE(metadata, '$.referral_code') as referral_code,
        JSON_VALUE(metadata, '$.campaign') as campaign,
        JSON_VALUE(metadata, '$.platform') as platform,
        
        -- Data quality metrics
        LENGTH(COALESCE(user_id, '')) as user_id_length,
        LENGTH(COALESCE(email, '')) as email_length,
        CASE 
            WHEN first_name IS NOT NULL AND last_name IS NOT NULL 
            THEN CONCAT(first_name, ' ', last_name)
            ELSE COALESCE(first_name, last_name, '')
        END as full_name,
        
        -- Standardized fields
        UPPER(TRIM(COALESCE(country, ''))) as country_standardized,
        LOWER(TRIM(COALESCE(subscription_tier, 'basic'))) as subscription_tier_standardized,
        
        -- User lifecycle metrics
        DATE_DIFF(CURRENT_DATE(), DATE(created_at), DAY) as days_since_registration,
        CASE 
            WHEN updated_at IS NOT NULL 
            THEN DATE_DIFF(CURRENT_DATE(), DATE(updated_at), DAY)
            ELSE NULL
        END as days_since_last_update
        
    FROM raw_users
)

SELECT
    user_id,
    email,
    created_at,
    updated_at,
    first_name,
    last_name,
    full_name,
    country,
    country_standardized,
    timezone,
    subscription_tier,
    subscription_tier_standardized,
    metadata,
    user_date,
    is_valid,
    validation_errors,
    source,
    referral_code,
    campaign,
    platform,
    user_id_length,
    email_length,
    days_since_registration,
    days_since_last_update,
    CURRENT_TIMESTAMP() as processed_at
FROM validated_users


