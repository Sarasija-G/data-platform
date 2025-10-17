-- Sample Analytics Queries for Data Platform
-- These queries demonstrate the key analytics capabilities of the platform

-- =============================================================================
-- 1. TOTAL EVENTS PER USER PER DAY
-- =============================================================================

-- Daily event counts per user
CREATE OR REPLACE VIEW `project_id.analytics.daily_user_events` AS
SELECT
    user_id,
    event_date,
    COUNT(*) as total_events,
    COUNT(DISTINCT event_type) as unique_event_types,
    COUNT(DISTINCT session_id) as unique_sessions,
    -- Event type breakdown
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_events,
    SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as page_view_events,
    SUM(CASE WHEN event_type = 'signup' THEN 1 ELSE 0 END) as signup_events,
    -- Revenue metrics
    SUM(CASE 
        WHEN event_type = 'purchase' AND JSON_VALUE(metadata, '$.amount') IS NOT NULL 
        THEN CAST(JSON_VALUE(metadata, '$.amount') AS FLOAT64) 
        ELSE 0 
    END) as total_revenue,
    -- Platform breakdown
    SUM(CASE WHEN platform = 'web' THEN 1 ELSE 0 END) as web_events,
    SUM(CASE WHEN platform = 'mobile' THEN 1 ELSE 0 END) as mobile_events
FROM `project_id.staging.events_clean`
WHERE is_valid = TRUE
GROUP BY user_id, event_date
ORDER BY user_id, event_date DESC;

-- Query to get specific user's daily events
SELECT 
    user_id,
    event_date,
    total_events,
    unique_event_types,
    total_revenue
FROM `project_id.analytics.daily_user_events`
WHERE user_id = 'user_001'
  AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY event_date DESC;

-- =============================================================================
-- 2. NUMBER OF ACTIVE DAYS PER USER IN A TIME RANGE
-- =============================================================================

-- User activity summary for a date range
CREATE OR REPLACE VIEW `project_id.analytics.user_activity_summary` AS
SELECT
    user_id,
    COUNT(DISTINCT event_date) as active_days,
    MIN(event_date) as first_activity_date,
    MAX(event_date) as last_activity_date,
    DATE_DIFF(MAX(event_date), MIN(event_date), DAY) + 1 as total_days_in_range,
    ROUND(COUNT(DISTINCT event_date) / (DATE_DIFF(MAX(event_date), MIN(event_date), DAY) + 1), 3) as activity_rate,
    -- Activity frequency categories
    CASE 
        WHEN COUNT(DISTINCT event_date) >= 20 THEN 'highly_active'
        WHEN COUNT(DISTINCT event_date) >= 10 THEN 'moderately_active'
        WHEN COUNT(DISTINCT event_date) >= 5 THEN 'occasionally_active'
        ELSE 'rarely_active'
    END as activity_category
FROM `project_id.staging.events_clean`
WHERE is_valid = TRUE
GROUP BY user_id;

-- Query to get active days for specific users in a time range
SELECT 
    user_id,
    active_days,
    total_days_in_range,
    activity_rate,
    activity_category
FROM `project_id.analytics.user_activity_summary`
WHERE user_id IN ('user_001', 'user_002', 'user_003')
  AND first_activity_date >= '2025-01-01'
  AND last_activity_date <= '2025-01-31'
ORDER BY active_days DESC;

-- =============================================================================
-- 3. TOP 3 EVENT TYPES PER USER
-- =============================================================================

-- User event type preferences
CREATE OR REPLACE VIEW `project_id.analytics.user_event_preferences` AS
WITH user_event_counts AS (
    SELECT
        user_id,
        event_type,
        COUNT(*) as event_count,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) as rank
    FROM `project_id.staging.events_clean`
    WHERE is_valid = TRUE
    GROUP BY user_id, event_type
),
top_3_per_user AS (
    SELECT
        user_id,
        event_type,
        event_count,
        rank,
        ROUND(event_count * 100.0 / SUM(event_count) OVER (PARTITION BY user_id), 2) as percentage
    FROM user_event_counts
    WHERE rank <= 3
)
SELECT 
    user_id,
    ARRAY_AGG(
        STRUCT(
            event_type,
            event_count,
            percentage
        ) ORDER BY rank
    ) as top_3_event_types
FROM top_3_per_user
GROUP BY user_id;

-- Query to get top event types for specific users
SELECT 
    user_id,
    top_3_event_types
FROM `project_id.analytics.user_event_preferences`
WHERE user_id IN ('user_001', 'user_002', 'user_003')
ORDER BY user_id;

-- =============================================================================
-- 4. BUSINESS METRICS QUERIES
-- =============================================================================

-- Daily active users (DAU)
CREATE OR REPLACE VIEW `project_id.analytics.daily_active_users` AS
SELECT
    event_date,
    COUNT(DISTINCT user_id) as daily_active_users,
    COUNT(DISTINCT session_id) as daily_active_sessions,
    COUNT(*) as total_events,
    ROUND(COUNT(*) / COUNT(DISTINCT user_id), 2) as avg_events_per_user
FROM `project_id.staging.events_clean`
WHERE is_valid = TRUE
GROUP BY event_date
ORDER BY event_date DESC;

-- Revenue metrics
CREATE OR REPLACE VIEW `project_id.analytics.revenue_metrics` AS
SELECT
    event_date,
    COUNT(DISTINCT user_id) as purchasing_users,
    COUNT(*) as total_purchases,
    SUM(CAST(JSON_VALUE(metadata, '$.amount') AS FLOAT64)) as total_revenue,
    AVG(CAST(JSON_VALUE(metadata, '$.amount') AS FLOAT64)) as avg_purchase_amount,
    MIN(CAST(JSON_VALUE(metadata, '$.amount') AS FLOAT64)) as min_purchase_amount,
    MAX(CAST(JSON_VALUE(metadata, '$.amount') AS FLOAT64)) as max_purchase_amount
FROM `project_id.staging.events_clean`
WHERE is_valid = TRUE
  AND event_type = 'purchase'
  AND JSON_VALUE(metadata, '$.amount') IS NOT NULL
GROUP BY event_date
ORDER BY event_date DESC;

-- User cohort analysis (monthly cohorts)
CREATE OR REPLACE VIEW `project_id.analytics.user_cohorts` AS
WITH user_first_activity AS (
    SELECT
        user_id,
        DATE_TRUNC(MIN(event_date), MONTH) as cohort_month
    FROM `project_id.staging.events_clean`
    WHERE is_valid = TRUE
    GROUP BY user_id
),
monthly_activity AS (
    SELECT
        user_id,
        DATE_TRUNC(event_date, MONTH) as activity_month
    FROM `project_id.staging.events_clean`
    WHERE is_valid = TRUE
    GROUP BY user_id, DATE_TRUNC(event_date, MONTH)
)
SELECT
    c.cohort_month,
    COUNT(DISTINCT c.user_id) as cohort_size,
    COUNT(DISTINCT CASE WHEN m.activity_month = c.cohort_month THEN c.user_id END) as month_0_retention,
    COUNT(DISTINCT CASE WHEN m.activity_month = DATE_ADD(c.cohort_month, INTERVAL 1 MONTH) THEN c.user_id END) as month_1_retention,
    COUNT(DISTINCT CASE WHEN m.activity_month = DATE_ADD(c.cohort_month, INTERVAL 2 MONTH) THEN c.user_id END) as month_2_retention,
    COUNT(DISTINCT CASE WHEN m.activity_month = DATE_ADD(c.cohort_month, INTERVAL 3 MONTH) THEN c.user_id END) as month_3_retention
FROM user_first_activity c
LEFT JOIN monthly_activity m ON c.user_id = m.user_id
GROUP BY c.cohort_month
ORDER BY c.cohort_month DESC;

-- =============================================================================
-- 5. ML FEATURE ENGINEERING QUERIES
-- =============================================================================

-- User behavior features for ML
CREATE OR REPLACE VIEW `project_id.analytics.ml_user_features` AS
WITH user_metrics AS (
    SELECT
        user_id,
        event_date,
        -- Recency features
        DATE_DIFF(CURRENT_DATE(), MAX(event_date), DAY) as days_since_last_event,
        -- Frequency features
        COUNT(*) as events_today,
        COUNT(DISTINCT event_type) as unique_event_types_today,
        -- Revenue features
        SUM(CASE 
            WHEN event_type = 'purchase' AND JSON_VALUE(metadata, '$.amount') IS NOT NULL 
            THEN CAST(JSON_VALUE(metadata, '$.amount') AS FLOAT64) 
            ELSE 0 
        END) as revenue_today
    FROM `project_id.staging.events_clean`
    WHERE is_valid = TRUE
    GROUP BY user_id, event_date
),
rolling_metrics AS (
    SELECT
        user_id,
        event_date,
        days_since_last_event,
        events_today,
        unique_event_types_today,
        revenue_today,
        -- 7-day rolling metrics
        SUM(events_today) OVER (
            PARTITION BY user_id 
            ORDER BY event_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as events_last_7_days,
        SUM(revenue_today) OVER (
            PARTITION BY user_id 
            ORDER BY event_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as revenue_last_7_days,
        -- 30-day rolling metrics
        SUM(events_today) OVER (
            PARTITION BY user_id 
            ORDER BY event_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as events_last_30_days,
        SUM(revenue_today) OVER (
            PARTITION BY user_id 
            ORDER BY event_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as revenue_last_30_days
    FROM user_metrics
)
SELECT
    user_id,
    event_date,
    days_since_last_event,
    events_today,
    unique_event_types_today,
    revenue_today,
    events_last_7_days,
    revenue_last_7_days,
    events_last_30_days,
    revenue_last_30_days,
    -- Derived features
    CASE 
        WHEN events_last_7_days > 0 THEN ROUND(revenue_last_7_days / events_last_7_days, 2)
        ELSE 0 
    END as avg_revenue_per_event_7d,
    CASE 
        WHEN events_last_30_days > 0 THEN ROUND(revenue_last_30_days / events_last_30_days, 2)
        ELSE 0 
    END as avg_revenue_per_event_30d
FROM rolling_metrics
ORDER BY user_id, event_date DESC;

-- =============================================================================
-- 6. PERFORMANCE OPTIMIZATION QUERIES
-- =============================================================================

-- Query to demonstrate partition pruning and clustering benefits
-- This query will only scan recent partitions and benefit from clustering
SELECT
    user_id,
    event_type,
    COUNT(*) as event_count,
    SUM(CASE 
        WHEN JSON_VALUE(metadata, '$.amount') IS NOT NULL 
        THEN CAST(JSON_VALUE(metadata, '$.amount') AS FLOAT64) 
        ELSE 0 
    END) as total_revenue
FROM `project_id.staging.events_clean`
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)  -- Partition pruning
  AND user_id IN ('user_001', 'user_002', 'user_003')        -- Clustering benefit
  AND event_type = 'purchase'                                 -- Clustering benefit
  AND is_valid = TRUE
GROUP BY user_id, event_type
ORDER BY total_revenue DESC;

-- =============================================================================
-- 7. DATA QUALITY VALIDATION QUERIES
-- =============================================================================

-- Data quality summary
CREATE OR REPLACE VIEW `project_id.analytics.data_quality_summary` AS
SELECT
    'events' as table_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) as valid_rows,
    SUM(CASE WHEN is_valid = FALSE THEN 1 ELSE 0 END) as invalid_rows,
    ROUND(SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as validity_percentage,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT event_type) as unique_event_types,
    MIN(event_date) as earliest_date,
    MAX(event_date) as latest_date
FROM `project_id.staging.events_clean`
UNION ALL
SELECT
    'users' as table_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) as valid_rows,
    SUM(CASE WHEN is_valid = FALSE THEN 1 ELSE 0 END) as invalid_rows,
    ROUND(SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as validity_percentage,
    COUNT(DISTINCT user_id) as unique_users,
    NULL as unique_event_types,
    MIN(user_date) as earliest_date,
    MAX(user_date) as latest_date
FROM `project_id.staging.users_clean`;

-- Query to check for data quality issues
SELECT 
    table_name,
    total_rows,
    valid_rows,
    invalid_rows,
    validity_percentage,
    unique_users,
    earliest_date,
    latest_date
FROM `project_id.analytics.data_quality_summary`
ORDER BY table_name;


