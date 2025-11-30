-- ============================================================================
-- MATERIALIZED VIEWS FOR DASHBOARD
-- ============================================================================
-- These views cache expensive queries for fast dashboard loading.
-- Refresh periodically via cron or pg_cron extension.
--
-- Performance: 1M+ rows query time
--   - Regular view: 5-15 seconds
--   - Materialized view: 10-50 milliseconds
-- ============================================================================

-- ============================================================================
-- 1. Main Dashboard Stats
-- Refresh: Every 5 minutes
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS zen_mv_dashboard;

CREATE MATERIALIZED VIEW zen_mv_dashboard AS
SELECT
    -- Row counts
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE scrape_status = 'success') AS success_count,
    COUNT(*) FILTER (WHERE scrape_status = 'failed') AS failed_count,
    COUNT(*) FILTER (WHERE scrape_status = 'pending') AS pending_count,
    
    -- Contact totals
    COALESCE(SUM(emails_count), 0) AS total_emails,
    COALESCE(SUM(phones_count), 0) AS total_phones,
    COALESCE(SUM(whatsapp_count), 0) AS total_whatsapp,
    
    -- Rows with contacts
    COUNT(*) FILTER (WHERE has_email = TRUE) AS rows_with_email,
    COUNT(*) FILTER (WHERE has_phone = TRUE) AS rows_with_phone,
    COUNT(*) FILTER (WHERE has_whatsapp = TRUE) AS rows_with_whatsapp,
    
    -- Social media
    COUNT(*) FILTER (WHERE social_facebook IS NOT NULL) AS rows_with_facebook,
    COUNT(*) FILTER (WHERE social_instagram IS NOT NULL) AS rows_with_instagram,
    
    -- Metadata
    COUNT(DISTINCT country_code) AS countries_count,
    COUNT(DISTINCT last_scrape_server) AS active_servers,
    
    -- Recent activity
    COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '24 hours') AS processed_24h,
    COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '1 hour') AS processed_1h,
    
    -- Performance
    ROUND(AVG(scrape_time_seconds)::NUMERIC, 2) AS avg_scrape_time,
    
    -- Refresh timestamp
    NOW() AS refreshed_at
FROM zen_contacts;

-- Create unique index for CONCURRENTLY refresh
CREATE UNIQUE INDEX idx_mv_dashboard_pk ON zen_mv_dashboard (refreshed_at);


-- ============================================================================
-- 2. Country Progress Stats
-- Refresh: Every 5 minutes
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS zen_mv_country_stats;

CREATE MATERIALIZED VIEW zen_mv_country_stats AS
SELECT
    country_code,
    country_name,
    
    -- Counts
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE scrape_status = 'success') AS success_count,
    COUNT(*) FILTER (WHERE scrape_status = 'failed') AS failed_count,
    COUNT(*) FILTER (WHERE scrape_status = 'pending') AS pending_count,
    
    -- Progress
    ROUND(
        (COUNT(*) FILTER (WHERE scrape_status IN ('success', 'failed')))::NUMERIC 
        / NULLIF(COUNT(*), 0) * 100, 1
    ) AS progress_percent,
    
    -- Contacts
    COALESCE(SUM(emails_count), 0) AS total_emails,
    COALESCE(SUM(phones_count), 0) AS total_phones,
    COALESCE(SUM(whatsapp_count), 0) AS total_whatsapp,
    
    -- Rows with contacts
    COUNT(*) FILTER (WHERE has_email = TRUE) AS rows_with_email,
    COUNT(*) FILTER (WHERE has_whatsapp = TRUE) AS rows_with_whatsapp,
    
    -- Performance
    ROUND(AVG(scrape_time_seconds)::NUMERIC, 2) AS avg_scrape_time,
    
    -- Activity
    MAX(updated_at) AS last_activity,
    
    -- Timestamp
    NOW() AS refreshed_at
FROM zen_contacts
GROUP BY country_code, country_name
ORDER BY total_rows DESC;

-- Create unique index for CONCURRENTLY refresh
CREATE UNIQUE INDEX idx_mv_country_stats_pk ON zen_mv_country_stats (country_code);


-- ============================================================================
-- 3. Hourly Statistics (Last 7 Days)
-- Refresh: Every 1 hour
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS zen_mv_hourly_stats;

CREATE MATERIALIZED VIEW zen_mv_hourly_stats AS
SELECT
    date_trunc('hour', updated_at) AS hour,
    
    -- Counts
    COUNT(*) AS rows_processed,
    COUNT(*) FILTER (WHERE scrape_status = 'success') AS success_count,
    COUNT(*) FILTER (WHERE scrape_status = 'failed') AS failed_count,
    
    -- Contacts found
    COALESCE(SUM(emails_count), 0) AS emails_found,
    COALESCE(SUM(phones_count), 0) AS phones_found,
    COALESCE(SUM(whatsapp_count), 0) AS whatsapp_found,
    
    -- Performance
    ROUND(AVG(scrape_time_seconds)::NUMERIC, 2) AS avg_time,
    COUNT(DISTINCT last_scrape_server) AS active_servers,
    
    -- Timestamp
    NOW() AS refreshed_at
FROM zen_contacts
WHERE updated_at > NOW() - INTERVAL '7 days'
GROUP BY date_trunc('hour', updated_at)
ORDER BY hour DESC;

-- Create unique index for CONCURRENTLY refresh
CREATE UNIQUE INDEX idx_mv_hourly_stats_pk ON zen_mv_hourly_stats (hour);


-- ============================================================================
-- 4. Category Statistics
-- Refresh: Every 1 hour
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS zen_mv_category_stats;

CREATE MATERIALIZED VIEW zen_mv_category_stats AS
SELECT
    business_category,
    
    -- Counts
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE scrape_status = 'success') AS success_count,
    
    -- Contacts
    COALESCE(SUM(emails_count), 0) AS total_emails,
    COALESCE(SUM(whatsapp_count), 0) AS total_whatsapp,
    
    -- Email rate
    ROUND(
        (COUNT(*) FILTER (WHERE has_email = TRUE))::NUMERIC 
        / NULLIF(COUNT(*), 0) * 100, 1
    ) AS email_rate,
    
    -- Timestamp
    NOW() AS refreshed_at
FROM zen_contacts
WHERE business_category IS NOT NULL AND business_category != ''
GROUP BY business_category
HAVING COUNT(*) >= 100  -- Only show categories with 100+ rows
ORDER BY total_rows DESC
LIMIT 100;

-- Create unique index for CONCURRENTLY refresh
CREATE UNIQUE INDEX idx_mv_category_stats_pk ON zen_mv_category_stats (business_category);


-- ============================================================================
-- REFRESH FUNCTIONS
-- ============================================================================

-- Function to refresh all dashboard views
CREATE OR REPLACE FUNCTION refresh_dashboard_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY zen_mv_dashboard;
    REFRESH MATERIALIZED VIEW CONCURRENTLY zen_mv_country_stats;
    RAISE NOTICE 'Dashboard views refreshed at %', NOW();
END;
$$ LANGUAGE plpgsql;

-- Function to refresh hourly views (run less frequently)
CREATE OR REPLACE FUNCTION refresh_hourly_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY zen_mv_hourly_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY zen_mv_category_stats;
    RAISE NOTICE 'Hourly views refreshed at %', NOW();
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- CRON SCHEDULE (if using pg_cron extension)
-- ============================================================================

-- Uncomment these if pg_cron is installed:

-- Every 5 minutes: refresh main dashboard
-- SELECT cron.schedule('refresh-dashboard', '*/5 * * * *', 'SELECT refresh_dashboard_views();');

-- Every hour: refresh hourly stats
-- SELECT cron.schedule('refresh-hourly', '0 * * * *', 'SELECT refresh_hourly_views();');

-- List scheduled jobs:
-- SELECT * FROM cron.job;


-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT 'Materialized views created successfully!' AS status;

-- Show all materialized views
SELECT matviewname, ispopulated
FROM pg_matviews
WHERE schemaname = 'public'
AND matviewname LIKE 'zen_mv_%';
