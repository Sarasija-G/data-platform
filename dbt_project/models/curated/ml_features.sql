-- Curated model for ML features - ML-ready feature engineering
-- This model creates features for machine learning models

{{ config(
    materialized='incremental',
    partition_by={
        "field": "feature_date",
        "data_type": "date"
    },
    cluster_by=["user_id"],
    unique_key=["user_id", "feature_date"],
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
        WHERE date >= (SELECT MAX(feature_date) FROM {{ this }})
    {% endif %}
),

-- Calculate rolling features for ML
rolling_features AS (
    SELECT
        user_id,
        date as feature_date,
        country,
        user_tier,
        days_since_registration,
        
        -- Recency features
        DATE_DIFF(CURRENT_DATE(), date, DAY) as days_since_last_event,
        DATE_DIFF(CURRENT_DATE(), date, DAY) as days_since_last_purchase,
        
        -- Frequency features (7-day window)
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
        SUM(total_sessions) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as sessions_last_7_days,
        
        -- Frequency features (30-day window)
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
        SUM(total_sessions) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as sessions_last_30_days,
        
        -- Monetary features (7-day window)
        SUM(total_revenue) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as revenue_last_7_days,
        AVG(total_revenue) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_revenue_last_7_days,
        
        -- Monetary features (30-day window)
        SUM(total_revenue) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as revenue_last_30_days,
        AVG(total_revenue) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as avg_revenue_last_30_days,
        
        -- Lifetime features
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
        ) as lifetime_revenue,
        
        -- Engagement features
        SUM(page_views) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as page_views_last_30_days,
        SUM(signups) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as signups_last_30_days,
        SUM(logins) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as logins_last_30_days,
        
        -- Discovery features
        MAX(unique_products_viewed) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as max_products_viewed_30d,
        MAX(unique_categories_viewed) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as max_categories_viewed_30d,
        
        -- Platform diversity
        SUM(web_events) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as web_events_last_30_days,
        SUM(mobile_events) OVER (
            PARTITION BY user_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as mobile_events_last_30_days
        
    FROM user_metrics
),

-- Calculate derived features
derived_features AS (
    SELECT
        *,
        
        -- Engagement scores
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
        
        -- Revenue per event ratios
        CASE 
            WHEN events_last_7_days > 0 
            THEN ROUND(revenue_last_7_days / events_last_7_days, 4)
            ELSE 0
        END as revenue_per_event_7d,
        
        CASE 
            WHEN events_last_30_days > 0 
            THEN ROUND(revenue_last_30_days / events_last_30_days, 4)
            ELSE 0
        END as revenue_per_event_30d,
        
        -- Average purchase amounts
        CASE 
            WHEN purchases_last_7_days > 0 
            THEN ROUND(revenue_last_7_days / purchases_last_7_days, 2)
            ELSE 0
        END as avg_purchase_amount_7d,
        
        CASE 
            WHEN purchases_last_30_days > 0 
            THEN ROUND(revenue_last_30_days / purchases_last_30_days, 2)
            ELSE 0
        END as avg_purchase_amount_30d,
        
        -- Platform diversity score (0-1)
        CASE 
            WHEN (web_events_last_30_days + mobile_events_last_30_days) > 0 
            THEN ROUND(
                LEAST(web_events_last_30_days, mobile_events_last_30_days) * 2.0 / 
                (web_events_last_30_days + mobile_events_last_30_days), 3
            )
            ELSE 0
        END as platform_diversity_score,
        
        -- Activity consistency (variance in daily events)
        STDDEV(total_events) OVER (
            PARTITION BY user_id 
            ORDER BY feature_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as activity_consistency_30d,
        
        -- Purchase frequency
        CASE 
            WHEN purchases_last_30_days > 0 
            THEN ROUND(30.0 / purchases_last_30_days, 1)
            ELSE 999
        END as avg_days_between_purchases_30d,
        
        -- User lifecycle stage
        CASE 
            WHEN days_since_registration <= 7 THEN 'new'
            WHEN days_since_registration <= 30 THEN 'recent'
            WHEN days_since_registration <= 90 THEN 'established'
            ELSE 'mature'
        END as lifecycle_stage,
        
        -- Value tier
        CASE 
            WHEN lifetime_revenue >= 1000 THEN 'high_value'
            WHEN lifetime_revenue >= 100 THEN 'medium_value'
            WHEN lifetime_revenue > 0 THEN 'low_value'
            ELSE 'no_revenue'
        END as value_tier,
        
        -- Activity flags
        events_last_7_days > 0 as is_active_7d,
        events_last_30_days > 0 as is_active_30d,
        purchases_last_7_days > 0 as purchased_7d,
        purchases_last_30_days > 0 as purchased_30d
        
    FROM rolling_features
),

-- Final ML features with normalized values
final_features AS (
    SELECT
        user_id,
        feature_date,
        country,
        user_tier,
        days_since_registration,
        lifecycle_stage,
        value_tier,
        
        -- Recency features
        days_since_last_event,
        days_since_last_purchase,
        
        -- Frequency features
        events_last_7_days,
        events_last_30_days,
        purchases_last_7_days,
        purchases_last_30_days,
        sessions_last_7_days,
        sessions_last_30_days,
        lifetime_events,
        lifetime_purchases,
        
        -- Monetary features
        revenue_last_7_days,
        revenue_last_30_days,
        lifetime_revenue,
        avg_revenue_last_7_days,
        avg_revenue_last_30_days,
        revenue_per_event_7d,
        revenue_per_event_30d,
        avg_purchase_amount_7d,
        avg_purchase_amount_30d,
        
        -- Engagement features
        engagement_score_7d,
        engagement_score_30d,
        page_views_last_30_days,
        signups_last_30_days,
        logins_last_30_days,
        
        -- Discovery features
        max_products_viewed_30d,
        max_categories_viewed_30d,
        
        -- Platform features
        web_events_last_30_days,
        mobile_events_last_30_days,
        platform_diversity_score,
        
        -- Behavioral features
        activity_consistency_30d,
        avg_days_between_purchases_30d,
        
        -- Activity flags
        is_active_7d,
        is_active_30d,
        purchased_7d,
        purchased_30d,
        
        -- Data freshness
        CURRENT_TIMESTAMP() as last_updated
        
    FROM derived_features
)

SELECT * FROM final_features


