-- BigQuery Schema Definition for Data Platform
-- This file contains the DDL statements to create the data warehouse schema

-- =============================================================================
-- RAW LAYER - Raw event data as received from applications
-- =============================================================================

-- Raw events table - stores all events as received
CREATE OR REPLACE TABLE `project_id.raw.events` (
    event_id STRING NOT NULL,
    user_id STRING NOT NULL,
    event_type STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    session_id STRING,
    device_type STRING,
    platform STRING,
    app_version STRING,
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    -- Partitioning and clustering
    event_date DATE GENERATED ALWAYS AS (DATE(event_timestamp)) STORED
)
PARTITION BY event_date
CLUSTER BY user_id, event_type, platform
OPTIONS (
    description = "Raw event data from all applications",
    partition_expiration_days = 2555, -- 7 years
    require_partition_filter = true
);

-- Raw users table - user master data
CREATE OR REPLACE TABLE `project_id.raw.users` (
    user_id STRING NOT NULL,
    email STRING,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    first_name STRING,
    last_name STRING,
    country STRING,
    timezone STRING,
    subscription_tier STRING,
    metadata JSON,
    -- Partitioning
    user_date DATE GENERATED ALWAYS AS (DATE(created_at)) STORED
)
PARTITION BY user_date
CLUSTER BY country, subscription_tier
OPTIONS (
    description = "Raw user master data",
    partition_expiration_days = 2555
);

-- Raw sessions table - session tracking
CREATE OR REPLACE TABLE `project_id.raw.sessions` (
    session_id STRING NOT NULL,
    user_id STRING NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    duration_seconds INT64,
    page_views INT64,
    events_count INT64,
    device_type STRING,
    platform STRING,
    country STRING,
    metadata JSON,
    -- Partitioning
    session_date DATE GENERATED ALWAYS AS (DATE(started_at)) STORED
)
PARTITION BY session_date
CLUSTER BY user_id, platform
OPTIONS (
    description = "Raw session data",
    partition_expiration_days = 2555,
    require_partition_filter = true
);

-- =============================================================================
-- STAGING LAYER - Cleaned and validated data
-- =============================================================================

-- Cleaned events table
CREATE OR REPLACE TABLE `project_id.staging.events_clean` (
    event_id STRING NOT NULL,
    user_id STRING NOT NULL,
    event_type STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    session_id STRING,
    device_type STRING,
    platform STRING,
    app_version STRING,
    -- Parsed metadata fields
    amount FLOAT64,
    currency STRING,
    product_id STRING,
    category STRING,
    -- Data quality flags
    is_valid BOOLEAN,
    validation_errors ARRAY<STRING>,
    -- Partitioning
    event_date DATE NOT NULL
)
PARTITION BY event_date
CLUSTER BY user_id, event_type, platform
OPTIONS (
    description = "Cleaned and validated event data",
    partition_expiration_days = 1095, -- 3 years
    require_partition_filter = true
);

-- Cleaned users table
CREATE OR REPLACE TABLE `project_id.staging.users_clean` (
    user_id STRING NOT NULL,
    email STRING,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    first_name STRING,
    last_name STRING,
    country STRING,
    timezone STRING,
    subscription_tier STRING,
    -- Data quality flags
    is_valid BOOLEAN,
    validation_errors ARRAY<STRING>,
    -- Partitioning
    user_date DATE NOT NULL
)
PARTITION BY user_date
CLUSTER BY country, subscription_tier
OPTIONS (
    description = "Cleaned and validated user data",
    partition_expiration_days = 1095
);

-- =============================================================================
-- CURATED LAYER - Business-ready datasets
-- =============================================================================

-- User metrics - daily aggregations
CREATE OR REPLACE TABLE `project_id.curated.user_metrics` (
    user_id STRING NOT NULL,
    date DATE NOT NULL,
    -- Event counts
    total_events INT64,
    unique_event_types INT64,
    -- Purchase metrics
    total_purchases INT64,
    total_revenue FLOAT64,
    avg_purchase_amount FLOAT64,
    -- Session metrics
    total_sessions INT64,
    total_session_duration_seconds INT64,
    avg_session_duration_seconds FLOAT64,
    -- Engagement metrics
    page_views INT64,
    unique_days_active INT64,
    -- Data freshness
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY user_id
OPTIONS (
    description = "Daily user-level metrics and aggregations",
    partition_expiration_days = 730, -- 2 years
    require_partition_filter = true
);

-- Event metrics - event type aggregations
CREATE OR REPLACE TABLE `project_id.curated.event_metrics` (
    event_type STRING NOT NULL,
    date DATE NOT NULL,
    -- Counts
    total_events INT64,
    unique_users INT64,
    unique_sessions INT64,
    -- Revenue metrics (for purchase events)
    total_revenue FLOAT64,
    avg_revenue_per_event FLOAT64,
    -- Platform breakdown
    web_events INT64,
    mobile_events INT64,
    -- Data freshness
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY event_type
OPTIONS (
    description = "Daily event type metrics and aggregations",
    partition_expiration_days = 730,
    require_partition_filter = true
);

-- ML features - ML-ready feature set
CREATE OR REPLACE TABLE `project_id.curated.ml_features` (
    user_id STRING NOT NULL,
    feature_date DATE NOT NULL,
    -- Recency features
    days_since_last_event INT64,
    days_since_last_purchase INT64,
    -- Frequency features
    events_last_7_days INT64,
    events_last_30_days INT64,
    purchases_last_7_days INT64,
    purchases_last_30_days INT64,
    -- Monetary features
    total_revenue_last_7_days FLOAT64,
    total_revenue_last_30_days FLOAT64,
    avg_purchase_amount_last_30_days FLOAT64,
    -- Engagement features
    sessions_last_7_days INT64,
    sessions_last_30_days INT64,
    avg_session_duration_last_30_days FLOAT64,
    -- Behavioral features
    unique_event_types_last_30_days INT64,
    platform_diversity_score FLOAT64,
    -- Data freshness
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY feature_date
CLUSTER BY user_id
OPTIONS (
    description = "ML-ready features for user behavior analysis",
    partition_expiration_days = 365, -- 1 year
    require_partition_filter = true
);

-- =============================================================================
-- MART LAYER - Final consumption datasets
-- =============================================================================

-- User analytics - comprehensive user behavior
CREATE OR REPLACE TABLE `project_id.mart.user_analytics` (
    user_id STRING NOT NULL,
    date DATE NOT NULL,
    -- User profile
    user_tier STRING,
    country STRING,
    days_since_registration INT64,
    -- Activity metrics
    is_active_today BOOLEAN,
    is_active_this_week BOOLEAN,
    is_active_this_month BOOLEAN,
    -- Engagement scores
    engagement_score_7d FLOAT64,
    engagement_score_30d FLOAT64,
    -- Revenue metrics
    lifetime_revenue FLOAT64,
    revenue_this_month FLOAT64,
    -- Behavioral patterns
    preferred_platform STRING,
    preferred_event_type STRING,
    avg_events_per_session FLOAT64,
    -- Data freshness
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY user_id, user_tier
OPTIONS (
    description = "Comprehensive user analytics for dashboards and reporting",
    partition_expiration_days = 365,
    require_partition_filter = true
);

-- Business metrics - key performance indicators
CREATE OR REPLACE TABLE `project_id.mart.business_metrics` (
    date DATE NOT NULL,
    -- User metrics
    daily_active_users INT64,
    weekly_active_users INT64,
    monthly_active_users INT64,
    new_users INT64,
    -- Revenue metrics
    daily_revenue FLOAT64,
    monthly_revenue FLOAT64,
    avg_revenue_per_user FLOAT64,
    -- Engagement metrics
    avg_events_per_user FLOAT64,
    avg_sessions_per_user FLOAT64,
    -- Conversion metrics
    purchase_conversion_rate FLOAT64,
    signup_conversion_rate FLOAT64,
    -- Data freshness
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY date
OPTIONS (
    description = "Daily business KPIs and metrics",
    partition_expiration_days = 1095, -- 3 years
    require_partition_filter = true
);

-- ML training data - prepared datasets for ML models
CREATE OR REPLACE TABLE `project_id.mart.ml_training` (
    user_id STRING NOT NULL,
    feature_date DATE NOT NULL,
    -- Target variables
    will_purchase_next_7_days BOOLEAN,
    will_churn_next_30_days BOOLEAN,
    predicted_lifetime_value FLOAT64,
    -- Feature groups
    recency_features STRUCT<
        days_since_last_event INT64,
        days_since_last_purchase INT64,
        days_since_registration INT64
    >,
    frequency_features STRUCT<
        events_7d INT64,
        events_30d INT64,
        purchases_7d INT64,
        purchases_30d INT64
    >,
    monetary_features STRUCT<
        revenue_7d FLOAT64,
        revenue_30d FLOAT64,
        lifetime_revenue FLOAT64,
        avg_purchase_amount FLOAT64
    >,
    engagement_features STRUCT<
        sessions_7d INT64,
        sessions_30d INT64,
        avg_session_duration FLOAT64,
        platform_diversity FLOAT64
    >,
    -- Data freshness
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY feature_date
CLUSTER BY user_id
OPTIONS (
    description = "ML training datasets with features and targets",
    partition_expiration_days = 365,
    require_partition_filter = true
);

-- =============================================================================
-- VIEWS - Simplified access patterns
-- =============================================================================

-- Recent events view (last 30 days)
CREATE OR REPLACE VIEW `project_id.views.recent_events` AS
SELECT
    event_id,
    user_id,
    event_type,
    event_timestamp,
    session_id,
    device_type,
    platform,
    amount,
    currency,
    product_id,
    category
FROM `project_id.staging.events_clean`
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND is_valid = TRUE;

-- Active users view (users with activity in last 30 days)
CREATE OR REPLACE VIEW `project_id.views.active_users` AS
SELECT DISTINCT
    u.user_id,
    u.email,
    u.created_at,
    u.country,
    u.subscription_tier,
    MAX(e.event_timestamp) as last_activity
FROM `project_id.staging.users_clean` u
JOIN `project_id.staging.events_clean` e ON u.user_id = e.user_id
WHERE e.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND u.is_valid = TRUE
  AND e.is_valid = TRUE
GROUP BY 1, 2, 3, 4, 5;

-- =============================================================================
-- INDEXES AND CONSTRAINTS
-- =============================================================================

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_events_user_timestamp 
ON `project_id.staging.events_clean` (user_id, event_timestamp);

CREATE INDEX IF NOT EXISTS idx_events_type_timestamp 
ON `project_id.staging.events_clean` (event_type, event_timestamp);

-- =============================================================================
-- SAMPLE DATA INSERTION (for testing)
-- =============================================================================

-- Insert sample events
INSERT INTO `project_id.raw.events` 
(event_id, user_id, event_type, event_timestamp, session_id, device_type, platform, app_version, metadata)
VALUES
('evt_001', 'user_001', 'purchase', '2025-01-15 10:30:00', 'sess_001', 'mobile', 'ios', '1.2.3', '{"amount": 42.50, "currency": "USD", "product_id": "prod_123"}'),
('evt_002', 'user_001', 'page_view', '2025-01-15 10:25:00', 'sess_001', 'mobile', 'ios', '1.2.3', '{"page": "/products", "category": "electronics"}'),
('evt_003', 'user_002', 'signup', '2025-01-15 09:15:00', 'sess_002', 'desktop', 'web', '2.1.0', '{"source": "google_ads", "campaign": "summer_sale"}'),
('evt_004', 'user_001', 'purchase', '2025-01-14 16:45:00', 'sess_003', 'mobile', 'android', '1.2.3', '{"amount": 25.99, "currency": "USD", "product_id": "prod_456"}'),
('evt_005', 'user_003', 'page_view', '2025-01-15 11:20:00', 'sess_004', 'desktop', 'web', '2.1.0', '{"page": "/checkout", "category": "clothing"}');

-- Insert sample users
INSERT INTO `project_id.raw.users`
(user_id, email, created_at, first_name, last_name, country, timezone, subscription_tier, metadata)
VALUES
('user_001', 'john@example.com', '2025-01-01 08:00:00', 'John', 'Doe', 'US', 'America/New_York', 'premium', '{"source": "organic", "referral_code": "FRIEND123"}'),
('user_002', 'jane@example.com', '2025-01-10 14:30:00', 'Jane', 'Smith', 'CA', 'America/Toronto', 'basic', '{"source": "google_ads", "campaign": "winter_promo"}'),
('user_003', 'bob@example.com', '2025-01-12 09:15:00', 'Bob', 'Johnson', 'UK', 'Europe/London', 'premium', '{"source": "social_media", "platform": "facebook"}');


