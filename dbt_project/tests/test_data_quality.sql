-- Data Quality Tests for Data Platform
-- These tests ensure data integrity and quality across all models

-- Test 1: Check for null values in critical fields
SELECT 
    'stg_events' as model_name,
    'null_event_ids' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('stg_events') }}
WHERE event_id IS NULL OR event_id = ''

UNION ALL

SELECT 
    'stg_events' as model_name,
    'null_user_ids' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('stg_events') }}
WHERE user_id IS NULL OR user_id = ''

UNION ALL

SELECT 
    'stg_events' as model_name,
    'null_event_types' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('stg_events') }}
WHERE event_type IS NULL OR event_type = ''

UNION ALL

SELECT 
    'stg_users' as model_name,
    'null_user_ids' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('stg_users') }}
WHERE user_id IS NULL OR user_id = ''

UNION ALL

SELECT 
    'stg_users' as model_name,
    'null_emails' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('stg_users') }}
WHERE email IS NULL OR email = ''

UNION ALL

SELECT 
    'stg_users' as model_name,
    'invalid_emails' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('stg_users') }}
WHERE email IS NOT NULL 
  AND NOT REGEXP_CONTAINS(email, r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

UNION ALL

-- Test 2: Check for duplicate records
SELECT 
    'stg_events' as model_name,
    'duplicate_event_ids' as test_name,
    COUNT(*) - COUNT(DISTINCT event_id) as failed_rows
FROM {{ ref('stg_events') }}

UNION ALL

SELECT 
    'stg_users' as model_name,
    'duplicate_user_ids' as test_name,
    COUNT(*) - COUNT(DISTINCT user_id) as failed_rows
FROM {{ ref('stg_users') }}

UNION ALL

-- Test 3: Check for data freshness
SELECT 
    'stg_events' as model_name,
    'stale_data' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('stg_events') }}
WHERE event_date < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)

UNION ALL

-- Test 4: Check for invalid event types
SELECT 
    'stg_events' as model_name,
    'invalid_event_types' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('stg_events') }}
WHERE event_type NOT IN (
    'purchase', 'page_view', 'signup', 'login', 'logout', 
    'click', 'scroll', 'search', 'filter', 'sort',
    'error', 'exception', 'crash', 'session_start', 'session_end'
)

UNION ALL

-- Test 5: Check for negative values in numeric fields
SELECT 
    'stg_events' as model_name,
    'negative_amounts' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('stg_events') }}
WHERE amount < 0

UNION ALL

-- Test 6: Check for future dates
SELECT 
    'stg_events' as model_name,
    'future_event_timestamps' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('stg_events') }}
WHERE event_timestamp > CURRENT_TIMESTAMP()

UNION ALL

SELECT 
    'stg_users' as model_name,
    'future_created_at' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('stg_users') }}
WHERE created_at > CURRENT_TIMESTAMP()

UNION ALL

-- Test 7: Check for data consistency in curated models
SELECT 
    'user_metrics' as model_name,
    'negative_events' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('user_metrics') }}
WHERE total_events < 0

UNION ALL

SELECT 
    'user_metrics' as model_name,
    'negative_revenue' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('user_metrics') }}
WHERE total_revenue < 0

UNION ALL

-- Test 8: Check for data completeness in business metrics
SELECT 
    'business_metrics' as model_name,
    'missing_dau' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('business_metrics') }}
WHERE daily_active_users IS NULL OR daily_active_users < 0

UNION ALL

-- Test 9: Check for logical inconsistencies
SELECT 
    'user_metrics' as model_name,
    'inconsistent_purchases' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('user_metrics') }}
WHERE total_purchases > 0 AND total_revenue = 0

UNION ALL

-- Test 10: Check for data quality in ML features
SELECT 
    'ml_features' as model_name,
    'invalid_engagement_scores' as test_name,
    COUNT(*) as failed_rows
FROM {{ ref('ml_features') }}
WHERE engagement_score_7d < 0 OR engagement_score_7d > 100
   OR engagement_score_30d < 0 OR engagement_score_30d > 100


