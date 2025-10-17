-- Curated model for event metrics - daily aggregations per event type
-- This model implements incremental processing for cost optimization

{{ config(
    materialized='incremental',
    partition_by={
        "field": "date",
        "data_type": "date"
    },
    cluster_by=["event_type"],
    unique_key=["event_type", "date"],
    on_schema_change="sync_all_columns"
) }}

WITH daily_events AS (
    SELECT
        event_type,
        event_date as date,
        user_id,
        session_id,
        amount,
        currency,
        product_id,
        category,
        platform,
        device_type
    FROM {{ ref('stg_events') }}
    WHERE is_valid = TRUE
    {% if is_incremental() %}
        -- Only process new data for incremental runs
        AND event_date >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

-- Daily aggregations per event type
daily_event_metrics AS (
    SELECT
        event_type,
        date,
        
        -- Count metrics
        COUNT(*) as total_events,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT session_id) as unique_sessions,
        
        -- Revenue metrics (for purchase events)
        SUM(CASE 
            WHEN event_type = 'purchase' AND amount IS NOT NULL 
            THEN amount 
            ELSE 0 
        END) as total_revenue,
        AVG(CASE 
            WHEN event_type = 'purchase' AND amount IS NOT NULL 
            THEN amount 
            ELSE NULL 
        END) as avg_revenue_per_event,
        MIN(CASE 
            WHEN event_type = 'purchase' AND amount IS NOT NULL 
            THEN amount 
            ELSE NULL 
        END) as min_revenue_per_event,
        MAX(CASE 
            WHEN event_type = 'purchase' AND amount IS NOT NULL 
            THEN amount 
            ELSE NULL 
        END) as max_revenue_per_event,
        
        -- Platform breakdown
        SUM(CASE WHEN platform = 'web' THEN 1 ELSE 0 END) as web_events,
        SUM(CASE WHEN platform = 'mobile' THEN 1 ELSE 0 END) as mobile_events,
        
        -- Device breakdown
        SUM(CASE WHEN device_type = 'desktop' THEN 1 ELSE 0 END) as desktop_events,
        SUM(CASE WHEN device_type = 'mobile' THEN 1 ELSE 0 END) as mobile_device_events,
        SUM(CASE WHEN device_type = 'tablet' THEN 1 ELSE 0 END) as tablet_events,
        
        -- Product/category metrics
        COUNT(DISTINCT product_id) as unique_products,
        COUNT(DISTINCT category) as unique_categories,
        
        -- Currency breakdown
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
        SUM(CASE 
            WHEN event_type = 'purchase' AND currency = 'GBP' AND amount IS NOT NULL 
            THEN amount 
            ELSE 0 
        END) as revenue_gbp,
        
        -- Data quality metrics
        SUM(CASE WHEN amount IS NULL AND event_type = 'purchase' THEN 1 ELSE 0 END) as purchases_without_amount,
        SUM(CASE WHEN product_id IS NULL AND event_type = 'purchase' THEN 1 ELSE 0 END) as purchases_without_product,
        SUM(CASE WHEN category IS NULL AND event_type IN ('page_view', 'purchase') THEN 1 ELSE 0 END) as events_without_category,
        
        -- Engagement metrics
        ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT user_id), 2) as avg_events_per_user,
        ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT session_id), 2) as avg_events_per_session
        
    FROM daily_events
    GROUP BY event_type, date
),

-- Add event type categorization
event_categorization AS (
    SELECT
        event_type,
        date,
        total_events,
        unique_users,
        unique_sessions,
        total_revenue,
        avg_revenue_per_event,
        min_revenue_per_event,
        max_revenue_per_event,
        web_events,
        mobile_events,
        desktop_events,
        mobile_device_events,
        tablet_events,
        unique_products,
        unique_categories,
        revenue_usd,
        revenue_eur,
        revenue_gbp,
        purchases_without_amount,
        purchases_without_product,
        events_without_category,
        avg_events_per_user,
        avg_events_per_session,
        
        -- Event type categorization
        CASE 
            WHEN event_type = 'purchase' THEN 'revenue'
            WHEN event_type IN ('page_view', 'click', 'scroll') THEN 'engagement'
            WHEN event_type IN ('signup', 'login', 'logout') THEN 'authentication'
            WHEN event_type IN ('search', 'filter', 'sort') THEN 'discovery'
            WHEN event_type IN ('error', 'exception', 'crash') THEN 'technical'
            ELSE 'other'
        END as event_category,
        
        -- Revenue contribution
        CASE 
            WHEN event_type = 'purchase' AND total_revenue > 0 THEN 'high_value'
            WHEN event_type = 'purchase' AND total_revenue = 0 THEN 'low_value'
            WHEN event_type IN ('page_view', 'click') THEN 'engagement'
            ELSE 'other'
        END as value_category,
        
        -- Growth metrics (compared to previous day)
        LAG(total_events) OVER (PARTITION BY event_type ORDER BY date) as prev_day_events,
        LAG(unique_users) OVER (PARTITION BY event_type ORDER BY date) as prev_day_users,
        LAG(total_revenue) OVER (PARTITION BY event_type ORDER BY date) as prev_day_revenue,
        
        -- Data freshness
        CURRENT_TIMESTAMP() as last_updated
        
    FROM daily_event_metrics
),

-- Calculate growth rates
final_metrics AS (
    SELECT
        event_type,
        date,
        event_category,
        value_category,
        
        -- Count metrics
        total_events,
        unique_users,
        unique_sessions,
        
        -- Revenue metrics
        total_revenue,
        avg_revenue_per_event,
        min_revenue_per_event,
        max_revenue_per_event,
        
        -- Platform metrics
        web_events,
        mobile_events,
        desktop_events,
        mobile_device_events,
        tablet_events,
        
        -- Product metrics
        unique_products,
        unique_categories,
        
        -- Currency metrics
        revenue_usd,
        revenue_eur,
        revenue_gbp,
        
        -- Data quality metrics
        purchases_without_amount,
        purchases_without_product,
        events_without_category,
        
        -- Engagement metrics
        avg_events_per_user,
        avg_events_per_session,
        
        -- Growth metrics
        prev_day_events,
        prev_day_users,
        prev_day_revenue,
        
        CASE 
            WHEN prev_day_events > 0 
            THEN ROUND((total_events - prev_day_events) * 100.0 / prev_day_events, 2)
            ELSE NULL
        END as events_growth_rate,
        
        CASE 
            WHEN prev_day_users > 0 
            THEN ROUND((unique_users - prev_day_users) * 100.0 / prev_day_users, 2)
            ELSE NULL
        END as users_growth_rate,
        
        CASE 
            WHEN prev_day_revenue > 0 
            THEN ROUND((total_revenue - prev_day_revenue) * 100.0 / prev_day_revenue, 2)
            ELSE NULL
        END as revenue_growth_rate,
        
        last_updated
        
    FROM event_categorization
)

SELECT * FROM final_metrics


