-- Mart model for business metrics - key performance indicators
-- This model provides daily business KPIs for executive dashboards

{{ config(
    materialized='incremental',
    partition_by={
        "field": "date",
        "data_type": "date"
    },
    cluster_by=["date"],
    unique_key=["date"],
    on_schema_change="sync_all_columns"
) }}

WITH daily_user_activity AS (
    SELECT
        date,
        user_id,
        country,
        user_tier,
        total_events,
        total_purchases,
        total_revenue,
        is_active_today,
        days_since_registration
    FROM {{ ref('user_metrics') }}
    {% if is_incremental() %}
        WHERE date >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

-- Daily aggregations
daily_metrics AS (
    SELECT
        date,
        
        -- User metrics
        COUNT(DISTINCT user_id) as daily_active_users,
        COUNT(DISTINCT CASE WHEN days_since_registration = 0 THEN user_id END) as new_users,
        COUNT(DISTINCT CASE WHEN days_since_registration BETWEEN 1 AND 7 THEN user_id END) as new_users_this_week,
        COUNT(DISTINCT CASE WHEN days_since_registration BETWEEN 1 AND 30 THEN user_id END) as new_users_this_month,
        
        -- Revenue metrics
        SUM(total_revenue) as daily_revenue,
        SUM(total_purchases) as daily_purchases,
        AVG(CASE WHEN total_purchases > 0 THEN total_revenue / total_purchases ELSE NULL END) as avg_purchase_amount,
        
        -- Engagement metrics
        SUM(total_events) as total_events,
        AVG(total_events) as avg_events_per_user,
        AVG(CASE WHEN total_events > 0 THEN total_events ELSE 0 END) as avg_events_per_active_user,
        
        -- Geographic breakdown
        COUNT(DISTINCT CASE WHEN country = 'US' THEN user_id END) as users_us,
        COUNT(DISTINCT CASE WHEN country = 'CA' THEN user_id END) as users_ca,
        COUNT(DISTINCT CASE WHEN country = 'UK' THEN user_id END) as users_uk,
        COUNT(DISTINCT CASE WHEN country NOT IN ('US', 'CA', 'UK') THEN user_id END) as users_other,
        
        -- Tier breakdown
        COUNT(DISTINCT CASE WHEN user_tier = 'premium' THEN user_id END) as premium_users,
        COUNT(DISTINCT CASE WHEN user_tier = 'basic' THEN user_id END) as basic_users,
        
        -- Revenue by tier
        SUM(CASE WHEN user_tier = 'premium' THEN total_revenue ELSE 0 END) as revenue_premium,
        SUM(CASE WHEN user_tier = 'basic' THEN total_revenue ELSE 0 END) as revenue_basic
        
    FROM daily_user_activity
    GROUP BY date
),

-- Calculate rolling metrics
rolling_metrics AS (
    SELECT
        *,
        
        -- 7-day rolling averages
        AVG(daily_active_users) OVER (
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as weekly_avg_dau,
        AVG(daily_revenue) OVER (
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as weekly_avg_revenue,
        
        -- 30-day rolling averages
        AVG(daily_active_users) OVER (
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as monthly_avg_dau,
        AVG(daily_revenue) OVER (
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as monthly_avg_revenue,
        
        -- Weekly active users (7-day window)
        COUNT(DISTINCT user_id) OVER (
            PARTITION BY DATE_TRUNC(date, WEEK)
        ) as weekly_active_users,
        
        -- Monthly active users (30-day window)
        COUNT(DISTINCT user_id) OVER (
            PARTITION BY DATE_TRUNC(date, MONTH)
        ) as monthly_active_users
        
    FROM daily_metrics
),

-- Calculate conversion rates and other KPIs
final_metrics AS (
    SELECT
        date,
        
        -- User metrics
        daily_active_users,
        new_users,
        new_users_this_week,
        new_users_this_month,
        weekly_active_users,
        monthly_active_users,
        
        -- Revenue metrics
        daily_revenue,
        daily_purchases,
        avg_purchase_amount,
        weekly_avg_revenue,
        monthly_avg_revenue,
        
        -- Engagement metrics
        total_events,
        avg_events_per_user,
        avg_events_per_active_user,
        
        -- Geographic metrics
        users_us,
        users_ca,
        users_uk,
        users_other,
        ROUND(users_us * 100.0 / daily_active_users, 2) as pct_users_us,
        ROUND(users_ca * 100.0 / daily_active_users, 2) as pct_users_ca,
        ROUND(users_uk * 100.0 / daily_active_users, 2) as pct_users_uk,
        
        -- Tier metrics
        premium_users,
        basic_users,
        ROUND(premium_users * 100.0 / daily_active_users, 2) as pct_premium_users,
        ROUND(basic_users * 100.0 / daily_active_users, 2) as pct_basic_users,
        
        -- Revenue by tier
        revenue_premium,
        revenue_basic,
        ROUND(revenue_premium * 100.0 / NULLIF(daily_revenue, 0), 2) as pct_revenue_premium,
        ROUND(revenue_basic * 100.0 / NULLIF(daily_revenue, 0), 2) as pct_revenue_basic,
        
        -- Conversion rates
        ROUND(daily_purchases * 100.0 / daily_active_users, 2) as purchase_conversion_rate,
        ROUND(new_users * 100.0 / daily_active_users, 2) as new_user_rate,
        
        -- Revenue per user metrics
        ROUND(daily_revenue / NULLIF(daily_active_users, 0), 2) as revenue_per_user,
        ROUND(revenue_premium / NULLIF(premium_users, 0), 2) as revenue_per_premium_user,
        ROUND(revenue_basic / NULLIF(basic_users, 0), 2) as revenue_per_basic_user,
        
        -- Growth metrics (compared to previous day)
        LAG(daily_active_users) OVER (ORDER BY date) as prev_day_dau,
        LAG(daily_revenue) OVER (ORDER BY date) as prev_day_revenue,
        LAG(daily_purchases) OVER (ORDER BY date) as prev_day_purchases,
        
        -- Growth rates
        ROUND((daily_active_users - LAG(daily_active_users) OVER (ORDER BY date)) * 100.0 / 
              NULLIF(LAG(daily_active_users) OVER (ORDER BY date), 0), 2) as dau_growth_rate,
        ROUND((daily_revenue - LAG(daily_revenue) OVER (ORDER BY date)) * 100.0 / 
              NULLIF(LAG(daily_revenue) OVER (ORDER BY date), 0), 2) as revenue_growth_rate,
        
        -- Data freshness
        CURRENT_TIMESTAMP() as last_updated
        
    FROM rolling_metrics
)

SELECT * FROM final_metrics


