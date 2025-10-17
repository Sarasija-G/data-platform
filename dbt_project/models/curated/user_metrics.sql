-- Curated model for user metrics - daily aggregations per user
-- This model implements incremental processing for cost optimization

{{ config(
    materialized='incremental',
    partition_by={
        "field": "date",
        "data_type": "date"
    },
    cluster_by=["user_id"],
    unique_key=["user_id", "date"],
    on_schema_change="sync_all_columns"
) }}

WITH daily_events AS (
    SELECT
        user_id,
        event_date as date,
        event_type,
        amount,
        currency,
        product_id,
        category,
        platform,
        device_type,
        session_id
    FROM {{ ref('stg_events') }}
    WHERE is_valid = TRUE
    {% if is_incremental() %}
        -- Only process new data for incremental runs
        AND event_date >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

-- Daily aggregations per user
daily_metrics AS (
    SELECT
        user_id,
        date,
        
        -- Event counts
        COUNT(*) as total_events,
        COUNT(DISTINCT event_type) as unique_event_types,
        COUNT(DISTINCT session_id) as unique_sessions,
        
        -- Purchase metrics
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
        SUM(CASE 
            WHEN event_type = 'purchase' AND amount IS NOT NULL 
            THEN amount 
            ELSE 0 
        END) as total_revenue,
        AVG(CASE 
            WHEN event_type = 'purchase' AND amount IS NOT NULL 
            THEN amount 
            ELSE NULL 
        END) as avg_purchase_amount,
        
        -- Platform breakdown
        SUM(CASE WHEN platform = 'web' THEN 1 ELSE 0 END) as web_events,
        SUM(CASE WHEN platform = 'mobile' THEN 1 ELSE 0 END) as mobile_events,
        
        -- Device breakdown
        SUM(CASE WHEN device_type = 'desktop' THEN 1 ELSE 0 END) as desktop_events,
        SUM(CASE WHEN device_type = 'mobile' THEN 1 ELSE 0 END) as mobile_device_events,
        
        -- Event type breakdown
        SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as page_views,
        SUM(CASE WHEN event_type = 'signup' THEN 1 ELSE 0 END) as signups,
        SUM(CASE WHEN event_type = 'login' THEN 1 ELSE 0 END) as logins,
        
        -- Product/category metrics
        COUNT(DISTINCT product_id) as unique_products_viewed,
        COUNT(DISTINCT category) as unique_categories_viewed,
        
        -- Session metrics (approximated)
        COUNT(DISTINCT session_id) as total_sessions,
        -- Note: For accurate session duration, we'd need session start/end times
        
        -- Engagement metrics
        ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT session_id), 2) as avg_events_per_session,
        
        -- Revenue by currency
        SUM(CASE 
            WHEN event_type = 'purchase' AND currency = 'USD' AND amount IS NOT NULL 
            THEN amount 
            ELSE 0 
        END) as revenue_usd,
        SUM(CASE 
            WHEN event_type = 'purchase' AND currency = 'EUR' AND amount IS NOT NULL 
            THEN amount 
            ELSE 0 
        END) as revenue_eur,
        
        -- Data quality metrics
        SUM(CASE WHEN amount IS NULL AND event_type = 'purchase' THEN 1 ELSE 0 END) as purchases_without_amount,
        SUM(CASE WHEN product_id IS NULL AND event_type = 'purchase' THEN 1 ELSE 0 END) as purchases_without_product
        
    FROM daily_events
    GROUP BY user_id, date
),

-- Add user context from users table
user_context AS (
    SELECT
        user_id,
        country_standardized as country,
        subscription_tier_standardized as user_tier,
        days_since_registration
    FROM {{ ref('stg_users') }}
    WHERE is_valid = TRUE
),

-- Final metrics with user context
final_metrics AS (
    SELECT
        dm.user_id,
        dm.date,
        uc.country,
        uc.user_tier,
        uc.days_since_registration,
        
        -- Event metrics
        dm.total_events,
        dm.unique_event_types,
        dm.unique_sessions,
        
        -- Purchase metrics
        dm.total_purchases,
        dm.total_revenue,
        dm.avg_purchase_amount,
        
        -- Platform metrics
        dm.web_events,
        dm.mobile_events,
        dm.desktop_events,
        dm.mobile_device_events,
        
        -- Event type metrics
        dm.page_views,
        dm.signups,
        dm.logins,
        
        -- Product metrics
        dm.unique_products_viewed,
        dm.unique_categories_viewed,
        
        -- Session metrics
        dm.total_sessions,
        dm.avg_events_per_session,
        
        -- Revenue metrics
        dm.revenue_usd,
        dm.revenue_eur,
        
        -- Data quality metrics
        dm.purchases_without_amount,
        dm.purchases_without_product,
        
        -- Derived metrics
        CASE 
            WHEN dm.total_events > 0 THEN ROUND(dm.total_revenue / dm.total_events, 4)
            ELSE 0
        END as revenue_per_event,
        
        CASE 
            WHEN dm.total_sessions > 0 THEN ROUND(dm.total_events * 1.0 / dm.total_sessions, 2)
            ELSE 0
        END as events_per_session,
        
        -- Activity flags
        dm.total_events > 0 as is_active_today,
        dm.total_purchases > 0 as made_purchase_today,
        
        -- Data freshness
        CURRENT_TIMESTAMP() as last_updated
        
    FROM daily_metrics dm
    LEFT JOIN user_context uc ON dm.user_id = uc.user_id
)

SELECT * FROM final_metrics


