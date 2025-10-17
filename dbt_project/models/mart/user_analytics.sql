-- Mart model for user analytics - comprehensive user behavior analysis
-- This model provides final consumption-ready user analytics

{{ config(
    materialized='incremental',
    partition_by={
        "field": "date",
        "data_type": "date"
    },
    cluster_by=["user_id", "user_tier"],
    unique_key=["user_id", "date"],
    on_schema_change="sync_all_columns"
) }}

WITH user_metrics AS (
    SELECT
        user_id,
        date,
        country,
        user_tier,
        days_since_registration,
        total_events,
        unique_event_types,
        total_purchases,
        total_revenue,
        avg_purchase_amount,
        web_events,
        mobile_events,
        page_views,
        signups,
        logins,
        unique_products_viewed,
        unique_categories_viewed,
        total_sessions,
        avg_events_per_session,
        is_active_today,
        made_purchase_today
    FROM {{ ref('user_metrics') }}
    {% if is_incremental() %}
        WHERE date >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

-- Calculate rolling metrics for engagement scoring
rolling_metrics AS (
    SELECT
        user_id,
        date,
        country,
        user_tier,
        days_since_registration,
        
        -- Daily metrics
        total_events,
        unique_event_types,
        total_purchases,
        total_revenue,
        avg_purchase_amount,
        web_events,
        mobile_events,
        page_views,
        signups,
        logins,
        unique_products_viewed,
        unique_categories_viewed,
        total_sessions,
        avg_events_per_session,
        is_active_today,
        made_purchase_today,
        
        -- 7-day rolling metrics
        SUM(total_events) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as events_last_7_days,
        SUM(total_purchases) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as purchases_last_7_days,
        SUM(total_revenue) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as revenue_last_7_days,
        SUM(total_sessions) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as sessions_last_7_days,
        
        -- 30-day rolling metrics
        SUM(total_events) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as events_last_30_days,
        SUM(total_purchases) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as purchases_last_30_days,
        SUM(total_revenue) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as revenue_last_30_days,
        SUM(total_sessions) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as sessions_last_30_days,
        
        -- Lifetime metrics (cumulative)
        SUM(total_events) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS UNBOUNDED PRECEDING
        ) as lifetime_events,
        SUM(total_purchases) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS UNBOUNDED PRECEDING
        ) as lifetime_purchases,
        SUM(total_revenue) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS UNBOUNDED PRECEDING
        ) as lifetime_revenue
        
    FROM user_metrics
),

-- Calculate engagement scores and behavioral patterns
engagement_analysis AS (
    SELECT
        *,
        
        -- Engagement scores (0-100 scale)
        LEAST(100, GREATEST(0, 
            (events_last_7_days * 2) + 
            (purchases_last_7_days * 10) + 
            (sessions_last_7_days * 3)
        )) as engagement_score_7d,
        
        LEAST(100, GREATEST(0, 
            (events_last_30_days * 1) + 
            (purchases_last_30_days * 5) + 
            (sessions_last_30_days * 2)
        )) as engagement_score_30d,
        
        -- Activity flags
        events_last_7_days > 0 as is_active_this_week,
        events_last_30_days > 0 as is_active_this_month,
        
        -- Revenue patterns
        CASE 
            WHEN lifetime_purchases > 0 
            THEN ROUND(lifetime_revenue / lifetime_purchases, 2)
            ELSE 0
        END as avg_lifetime_purchase_amount,
        
        -- Platform preferences
        CASE 
            WHEN web_events > mobile_events THEN 'web'
            WHEN mobile_events > web_events THEN 'mobile'
            WHEN web_events = mobile_events AND web_events > 0 THEN 'balanced'
            ELSE 'unknown'
        END as preferred_platform,
        
        -- Event type preferences (simplified)
        CASE 
            WHEN page_views > total_events * 0.7 THEN 'browser'
            WHEN total_purchases > 0 THEN 'buyer'
            WHEN signups > 0 THEN 'new_user'
            ELSE 'explorer'
        END as user_behavior_type,
        
        -- Engagement trends
        CASE 
            WHEN events_last_7_days > events_last_30_days * 0.4 THEN 'increasing'
            WHEN events_last_7_days < events_last_30_days * 0.1 THEN 'decreasing'
            ELSE 'stable'
        END as engagement_trend,
        
        -- Value segmentation
        CASE 
            WHEN lifetime_revenue >= 1000 THEN 'high_value'
            WHEN lifetime_revenue >= 100 THEN 'medium_value'
            WHEN lifetime_revenue > 0 THEN 'low_value'
            ELSE 'no_revenue'
        END as value_segment,
        
        -- Churn risk indicators
        CASE 
            WHEN events_last_7_days = 0 AND events_last_30_days > 0 THEN 'at_risk'
            WHEN events_last_30_days = 0 AND lifetime_events > 0 THEN 'churned'
            WHEN events_last_7_days > 0 THEN 'active'
            ELSE 'inactive'
        END as churn_risk_status
        
    FROM rolling_metrics
),

-- Final analytics with derived insights
final_analytics AS (
    SELECT
        user_id,
        date,
        country,
        user_tier,
        days_since_registration,
        
        -- Daily activity
        is_active_today,
        is_active_this_week,
        is_active_this_month,
        
        -- Event metrics
        total_events,
        unique_event_types,
        events_last_7_days,
        events_last_30_days,
        lifetime_events,
        
        -- Purchase metrics
        total_purchases,
        total_revenue,
        avg_purchase_amount,
        purchases_last_7_days,
        purchases_last_30_days,
        lifetime_purchases,
        lifetime_revenue,
        avg_lifetime_purchase_amount,
        
        -- Session metrics
        total_sessions,
        avg_events_per_session,
        sessions_last_7_days,
        sessions_last_30_days,
        
        -- Engagement metrics
        engagement_score_7d,
        engagement_score_30d,
        engagement_trend,
        
        -- Behavioral patterns
        preferred_platform,
        user_behavior_type,
        value_segment,
        churn_risk_status,
        
        -- Platform breakdown
        web_events,
        mobile_events,
        
        -- Event type breakdown
        page_views,
        signups,
        logins,
        
        -- Discovery metrics
        unique_products_viewed,
        unique_categories_viewed,
        
        -- Data freshness
        CURRENT_TIMESTAMP() as last_updated
        
    FROM engagement_analysis
)

SELECT * FROM final_analytics


