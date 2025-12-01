-- ============================================================================
-- InsightHub ENRICHMENT DATABASE SCHEMA v3
-- ============================================================================
-- Complete schema untuk multi-country scraping dengan monitoring dashboard
--
-- Tables:
--   zen_contacts        - Main enriched contacts data (partitioned)
--   zen_servers         - Server registry untuk monitoring
--   zen_jobs            - Job execution history
--   zen_stats_hourly    - Hourly statistics
--
-- Views:
--   zen_v_dashboard          - Main dashboard view
--   zen_v_server_status      - Server status overview
--   zen_v_country_progress   - Progress per country
--   zen_v_recent_activity    - Recent scraping activity
--
-- Author: Claude (Sonnet 4)
-- Date: 2025-11-30
-- ============================================================================

-- ============================================================================
-- TABLE 1: zen_contacts (Main Data - Hash Partitioned)
-- ============================================================================

DROP TABLE IF EXISTS zen_contacts CASCADE;

CREATE TABLE zen_contacts (
    -- ========== IDENTITY ==========
    id BIGSERIAL,
    partition_key INTEGER NOT NULL DEFAULT 0,

    -- ========== SOURCE REFERENCE ==========
    source_id INTEGER,                        -- results.id
    source_link VARCHAR(2048) NOT NULL,       -- Google Maps URL (UPSERT key)

    -- ========== BUSINESS INFO ==========
    business_name VARCHAR(500),
    business_category VARCHAR(255),
    business_website VARCHAR(2048),

    -- ========== LOCATION ==========
    country_code VARCHAR(2) NOT NULL DEFAULT 'XX',  -- ISO 3166-1 alpha-2
    country_name VARCHAR(100),
    city VARCHAR(255),
    state VARCHAR(255),
    address TEXT,
    postal_code VARCHAR(20),
    latitude NUMERIC(10, 8),
    longitude NUMERIC(11, 8),
    timezone VARCHAR(100),

    -- ========== GOOGLE MAPS DATA ==========
    gmaps_phone VARCHAR(100),
    gmaps_rating NUMERIC(3, 2),
    gmaps_review_count INTEGER,
    gmaps_price_range VARCHAR(50),
    gmaps_cid VARCHAR(255),
    gmaps_status VARCHAR(50),

    -- ========== ENRICHED: EMAILS ==========
    emails TEXT[],
    emails_count INTEGER DEFAULT 0,
    emails_validated JSONB,                   -- Validation results
    has_email BOOLEAN GENERATED ALWAYS AS (emails_count > 0) STORED,

    -- ========== ENRICHED: PHONES ==========
    phones TEXT[],
    phones_count INTEGER DEFAULT 0,
    has_phone BOOLEAN GENERATED ALWAYS AS (phones_count > 0) STORED,

    -- ========== ENRICHED: WHATSAPP ==========
    whatsapp TEXT[],
    whatsapp_count INTEGER DEFAULT 0,
    whatsapp_validated JSONB,
    has_whatsapp BOOLEAN GENERATED ALWAYS AS (whatsapp_count > 0) STORED,

    -- ========== ENRICHED: SOCIAL MEDIA ==========
    social_facebook VARCHAR(500),
    social_instagram VARCHAR(500),
    social_linkedin VARCHAR(500),
    social_tiktok VARCHAR(500),
    social_youtube VARCHAR(500),
    social_twitter VARCHAR(500),
    social_count INTEGER DEFAULT 0,

    -- ========== SCRAPING METADATA ==========
    scrape_status VARCHAR(20) DEFAULT 'pending',  -- pending, success, failed, skipped
    scrape_error TEXT,
    scrape_final_url VARCHAR(2048),
    scrape_was_redirected BOOLEAN DEFAULT FALSE,
    scrape_time_seconds NUMERIC(10, 3),
    scrape_pages_count INTEGER DEFAULT 0,

    -- ========== AUDIT ==========
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    scrape_count INTEGER DEFAULT 0,
    last_scrape_server VARCHAR(50),
    last_scrape_at TIMESTAMPTZ,

    -- ========== CONSTRAINTS ==========
    PRIMARY KEY (id, partition_key),
    UNIQUE (source_link, partition_key)

) PARTITION BY HASH (partition_key);

-- Create 32 partitions
DO $$
BEGIN
    FOR i IN 0..31 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS zen_contacts_p%s PARTITION OF zen_contacts FOR VALUES WITH (MODULUS 32, REMAINDER %s)',
            i, i
        );
    END LOOP;
END $$;

-- Indexes for zen_contacts
CREATE INDEX idx_zc_country ON zen_contacts(country_code);
CREATE INDEX idx_zc_category ON zen_contacts(business_category);
CREATE INDEX idx_zc_country_category ON zen_contacts(country_code, business_category);
CREATE INDEX idx_zc_status ON zen_contacts(scrape_status);
CREATE INDEX idx_zc_country_status ON zen_contacts(country_code, scrape_status);
CREATE INDEX idx_zc_link_hash ON zen_contacts USING hash(source_link);
CREATE INDEX idx_zc_updated ON zen_contacts(updated_at DESC);
CREATE INDEX idx_zc_has_email ON zen_contacts(country_code) WHERE has_email = TRUE;
CREATE INDEX idx_zc_has_whatsapp ON zen_contacts(country_code) WHERE has_whatsapp = TRUE;
CREATE INDEX idx_zc_server ON zen_contacts(last_scrape_server);

-- Auto-update timestamp trigger
CREATE OR REPLACE FUNCTION zen_update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_zc_updated
    BEFORE UPDATE ON zen_contacts
    FOR EACH ROW
    EXECUTE FUNCTION zen_update_timestamp();


-- ============================================================================
-- TABLE 2: zen_servers (Server Registry for Monitoring)
-- ============================================================================

DROP TABLE IF EXISTS zen_servers CASCADE;

CREATE TABLE zen_servers (
    -- ========== IDENTITY ==========
    server_id VARCHAR(50) PRIMARY KEY,
    server_name VARCHAR(100),
    server_ip VARCHAR(45),
    server_hostname VARCHAR(255),
    server_region VARCHAR(50),               -- 'sg', 'id', 'us', etc.

    -- ========== CONFIGURATION ==========
    workers_count INTEGER DEFAULT 6,
    batch_size INTEGER DEFAULT 100,

    -- ========== STATUS ==========
    status VARCHAR(20) DEFAULT 'offline',    -- online, offline, paused, error
    current_task VARCHAR(255),               -- What it's currently doing

    -- ========== STATISTICS ==========
    total_processed BIGINT DEFAULT 0,
    total_success BIGINT DEFAULT 0,
    total_failed BIGINT DEFAULT 0,
    total_emails_found BIGINT DEFAULT 0,
    total_phones_found BIGINT DEFAULT 0,
    total_whatsapp_found BIGINT DEFAULT 0,

    -- ========== PERFORMANCE ==========
    avg_time_per_url NUMERIC(10, 3),         -- Average seconds per URL
    urls_per_minute NUMERIC(10, 2),          -- Current rate
    success_rate NUMERIC(5, 2),              -- Success percentage

    -- ========== TIMESTAMPS ==========
    started_at TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ,
    last_activity TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- ========== SESSION INFO ==========
    session_id VARCHAR(100),                 -- Unique per run
    session_started TIMESTAMPTZ,
    session_processed INTEGER DEFAULT 0,
    session_errors INTEGER DEFAULT 0
);

CREATE INDEX idx_zs_status ON zen_servers(status);
CREATE INDEX idx_zs_heartbeat ON zen_servers(last_heartbeat DESC);
CREATE INDEX idx_zs_region ON zen_servers(server_region);


-- ============================================================================
-- TABLE 3: zen_jobs (Job Execution History)
-- ============================================================================

DROP TABLE IF EXISTS zen_jobs CASCADE;

CREATE TABLE zen_jobs (
    -- ========== IDENTITY ==========
    id BIGSERIAL PRIMARY KEY,
    job_id VARCHAR(100) UNIQUE,              -- UUID or custom ID

    -- ========== JOB INFO ==========
    server_id VARCHAR(50) REFERENCES zen_servers(server_id),
    job_type VARCHAR(50),                    -- 'dsn_batch', 'csv_import', 'rescrape'
    job_status VARCHAR(20) DEFAULT 'running', -- running, completed, failed, cancelled

    -- ========== SCOPE ==========
    country_filter VARCHAR(2),               -- NULL = all countries
    category_filter VARCHAR(255),
    batch_size INTEGER,

    -- ========== PROGRESS ==========
    total_rows INTEGER DEFAULT 0,
    processed_rows INTEGER DEFAULT 0,
    success_rows INTEGER DEFAULT 0,
    failed_rows INTEGER DEFAULT 0,
    skipped_rows INTEGER DEFAULT 0,
    progress_percent NUMERIC(5, 2) GENERATED ALWAYS AS (
        CASE WHEN total_rows > 0 THEN (processed_rows::NUMERIC / total_rows * 100) ELSE 0 END
    ) STORED,

    -- ========== RESULTS ==========
    emails_found INTEGER DEFAULT 0,
    phones_found INTEGER DEFAULT 0,
    whatsapp_found INTEGER DEFAULT 0,

    -- ========== TIMING ==========
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_seconds INTEGER,
    avg_time_per_row NUMERIC(10, 3),

    -- ========== ERROR TRACKING ==========
    last_error TEXT,
    error_count INTEGER DEFAULT 0
);

CREATE INDEX idx_zj_server ON zen_jobs(server_id);
CREATE INDEX idx_zj_status ON zen_jobs(job_status);
CREATE INDEX idx_zj_started ON zen_jobs(started_at DESC);
CREATE INDEX idx_zj_country ON zen_jobs(country_filter);


-- ============================================================================
-- TABLE 4: zen_stats_hourly (Hourly Statistics for Dashboard)
-- ============================================================================

DROP TABLE IF EXISTS zen_stats_hourly CASCADE;

CREATE TABLE zen_stats_hourly (
    -- ========== IDENTITY ==========
    id BIGSERIAL PRIMARY KEY,
    stat_hour TIMESTAMPTZ NOT NULL,          -- Truncated to hour
    country_code VARCHAR(2) DEFAULT 'ALL',   -- 'ALL' for global stats

    -- ========== COUNTS ==========
    rows_processed INTEGER DEFAULT 0,
    rows_success INTEGER DEFAULT 0,
    rows_failed INTEGER DEFAULT 0,

    -- ========== CONTACT STATS ==========
    emails_found INTEGER DEFAULT 0,
    phones_found INTEGER DEFAULT 0,
    whatsapp_found INTEGER DEFAULT 0,

    -- ========== PERFORMANCE ==========
    avg_time_seconds NUMERIC(10, 3),
    total_time_seconds NUMERIC(12, 3),

    -- ========== UNIQUE CONSTRAINT ==========
    UNIQUE(stat_hour, country_code)
);

CREATE INDEX idx_zsh_hour ON zen_stats_hourly(stat_hour DESC);
CREATE INDEX idx_zsh_country ON zen_stats_hourly(country_code);


-- ============================================================================
-- VIEW 1: zen_v_dashboard (Main Dashboard Overview)
-- ============================================================================

CREATE OR REPLACE VIEW zen_v_dashboard AS
SELECT
    -- Total counts
    (SELECT COUNT(*) FROM results) AS source_total,
    (SELECT COUNT(*) FROM zen_contacts) AS enriched_total,
    (SELECT COUNT(*) FROM zen_contacts WHERE scrape_status = 'success') AS enriched_success,
    (SELECT COUNT(*) FROM zen_contacts WHERE scrape_status = 'failed') AS enriched_failed,
    (SELECT COUNT(*) FROM zen_contacts WHERE scrape_status = 'pending') AS enriched_pending,

    -- Pending calculation
    (SELECT COUNT(*) FROM results r WHERE NOT EXISTS (
        SELECT 1 FROM zen_contacts zc WHERE zc.source_link = r.data->>'link'
    )) AS pending_total,

    -- Contact totals
    (SELECT COALESCE(SUM(emails_count), 0) FROM zen_contacts) AS total_emails,
    (SELECT COALESCE(SUM(phones_count), 0) FROM zen_contacts) AS total_phones,
    (SELECT COALESCE(SUM(whatsapp_count), 0) FROM zen_contacts) AS total_whatsapp,

    -- Unique counts
    (SELECT COUNT(*) FROM zen_contacts WHERE has_email) AS rows_with_email,
    (SELECT COUNT(*) FROM zen_contacts WHERE has_phone) AS rows_with_phone,
    (SELECT COUNT(*) FROM zen_contacts WHERE has_whatsapp) AS rows_with_whatsapp,

    -- Country stats
    (SELECT COUNT(DISTINCT country_code) FROM zen_contacts) AS countries_processed,

    -- Server stats
    (SELECT COUNT(*) FROM zen_servers WHERE status = 'online') AS servers_online,
    (SELECT COUNT(*) FROM zen_servers) AS servers_total,

    -- Recent activity (last 24h)
    (SELECT COUNT(*) FROM zen_contacts WHERE updated_at > NOW() - INTERVAL '24 hours') AS processed_24h,
    (SELECT COUNT(*) FROM zen_contacts WHERE updated_at > NOW() - INTERVAL '1 hour') AS processed_1h,

    -- Timestamp
    NOW() AS generated_at;


-- ============================================================================
-- VIEW 2: zen_v_server_status (Server Monitoring)
-- ============================================================================

CREATE OR REPLACE VIEW zen_v_server_status AS
SELECT
    s.server_id,
    s.server_name,
    s.server_region,
    s.status,
    s.workers_count,
    s.current_task,

    -- Health check
    CASE
        WHEN s.last_heartbeat > NOW() - INTERVAL '2 minutes' THEN 'healthy'
        WHEN s.last_heartbeat > NOW() - INTERVAL '5 minutes' THEN 'warning'
        ELSE 'critical'
    END AS health,

    -- Time since last heartbeat
    EXTRACT(EPOCH FROM (NOW() - s.last_heartbeat))::INTEGER AS seconds_since_heartbeat,

    -- Statistics
    s.total_processed,
    s.total_success,
    s.total_failed,
    s.success_rate,
    s.urls_per_minute,
    s.avg_time_per_url,

    -- Session info
    s.session_processed,
    s.session_errors,
    EXTRACT(EPOCH FROM (NOW() - s.session_started))::INTEGER AS session_duration_seconds,

    -- Timestamps
    s.last_heartbeat,
    s.last_activity,
    s.started_at

FROM zen_servers s
ORDER BY
    CASE s.status
        WHEN 'online' THEN 1
        WHEN 'paused' THEN 2
        WHEN 'error' THEN 3
        ELSE 4
    END,
    s.last_heartbeat DESC NULLS LAST;


-- ============================================================================
-- VIEW 3: zen_v_country_progress (Progress per Country)
-- ============================================================================

CREATE OR REPLACE VIEW zen_v_country_progress AS
WITH source_counts AS (
    SELECT
        UPPER(LEFT(COALESCE(r.data->'complete_address'->>'country', 'XX'), 2)) AS country_code,
        COUNT(*) AS source_count
    FROM results r
    GROUP BY 1
),
enriched_counts AS (
    SELECT
        country_code,
        COUNT(*) AS enriched_count,
        COUNT(*) FILTER (WHERE scrape_status = 'success') AS success_count,
        COUNT(*) FILTER (WHERE scrape_status = 'failed') AS failed_count,
        SUM(emails_count) AS emails_total,
        SUM(phones_count) AS phones_total,
        SUM(whatsapp_count) AS whatsapp_total,
        COUNT(*) FILTER (WHERE has_email) AS rows_with_email,
        COUNT(*) FILTER (WHERE has_whatsapp) AS rows_with_whatsapp,
        AVG(scrape_time_seconds) AS avg_scrape_time,
        MAX(updated_at) AS last_activity
    FROM zen_contacts
    GROUP BY country_code
)
SELECT
    COALESCE(s.country_code, e.country_code) AS country_code,
    COALESCE(s.source_count, 0) AS source_total,
    COALESCE(e.enriched_count, 0) AS enriched_total,
    COALESCE(s.source_count, 0) - COALESCE(e.enriched_count, 0) AS pending,
    CASE
        WHEN COALESCE(s.source_count, 0) > 0
        THEN ROUND((COALESCE(e.enriched_count, 0)::NUMERIC / s.source_count * 100), 1)
        ELSE 0
    END AS progress_percent,
    COALESCE(e.success_count, 0) AS success_count,
    COALESCE(e.failed_count, 0) AS failed_count,
    COALESCE(e.emails_total, 0) AS emails_total,
    COALESCE(e.phones_total, 0) AS phones_total,
    COALESCE(e.whatsapp_total, 0) AS whatsapp_total,
    COALESCE(e.rows_with_email, 0) AS rows_with_email,
    COALESCE(e.rows_with_whatsapp, 0) AS rows_with_whatsapp,
    ROUND(COALESCE(e.avg_scrape_time, 0)::NUMERIC, 2) AS avg_scrape_time,
    e.last_activity
FROM source_counts s
FULL OUTER JOIN enriched_counts e ON s.country_code = e.country_code
ORDER BY COALESCE(s.source_count, 0) DESC;


-- ============================================================================
-- VIEW 4: zen_v_recent_activity (Recent Scraping Activity)
-- ============================================================================

CREATE OR REPLACE VIEW zen_v_recent_activity AS
SELECT
    id,
    country_code,
    business_name,
    business_category,
    scrape_status,
    emails_count,
    phones_count,
    whatsapp_count,
    social_facebook IS NOT NULL AS has_facebook,
    social_instagram IS NOT NULL AS has_instagram,
    scrape_time_seconds,
    last_scrape_server,
    updated_at
FROM zen_contacts
ORDER BY updated_at DESC
LIMIT 500;


-- ============================================================================
-- VIEW 5: zen_v_hourly_stats (Hourly Performance)
-- ============================================================================

CREATE OR REPLACE VIEW zen_v_hourly_stats AS
SELECT
    date_trunc('hour', updated_at) AS hour,
    COUNT(*) AS rows_processed,
    COUNT(*) FILTER (WHERE scrape_status = 'success') AS success,
    COUNT(*) FILTER (WHERE scrape_status = 'failed') AS failed,
    SUM(emails_count) AS emails_found,
    SUM(whatsapp_count) AS whatsapp_found,
    ROUND(AVG(scrape_time_seconds)::NUMERIC, 2) AS avg_time,
    COUNT(DISTINCT last_scrape_server) AS active_servers
FROM zen_contacts
WHERE updated_at > NOW() - INTERVAL '7 days'
GROUP BY 1
ORDER BY 1 DESC;


-- ============================================================================
-- FUNCTION: zen_get_partition_key (Calculate partition from link)
-- ============================================================================

CREATE OR REPLACE FUNCTION zen_get_partition_key(link_url TEXT)
RETURNS INTEGER AS $$
BEGIN
    RETURN ABS(hashtext(link_url)) % 32;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- ============================================================================
-- FUNCTION: zen_update_server_stats (Call after each batch)
-- ============================================================================

CREATE OR REPLACE FUNCTION zen_update_server_stats(
    p_server_id VARCHAR(50),
    p_processed INTEGER,
    p_success INTEGER,
    p_failed INTEGER,
    p_emails INTEGER,
    p_phones INTEGER,
    p_whatsapp INTEGER,
    p_avg_time NUMERIC
)
RETURNS VOID AS $$
BEGIN
    UPDATE zen_servers SET
        total_processed = total_processed + p_processed,
        total_success = total_success + p_success,
        total_failed = total_failed + p_failed,
        total_emails_found = total_emails_found + p_emails,
        total_phones_found = total_phones_found + p_phones,
        total_whatsapp_found = total_whatsapp_found + p_whatsapp,
        session_processed = session_processed + p_processed,
        avg_time_per_url = (avg_time_per_url * total_processed + p_avg_time * p_processed) / (total_processed + p_processed),
        success_rate = (total_success + p_success)::NUMERIC / NULLIF(total_processed + p_processed, 0) * 100,
        last_activity = NOW(),
        last_heartbeat = NOW()
    WHERE server_id = p_server_id;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- FUNCTION: zen_server_heartbeat (Call every 30s from each server)
-- ============================================================================

CREATE OR REPLACE FUNCTION zen_server_heartbeat(
    p_server_id VARCHAR(50),
    p_current_task VARCHAR(255) DEFAULT NULL,
    p_urls_per_minute NUMERIC DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    UPDATE zen_servers SET
        last_heartbeat = NOW(),
        status = 'online',
        current_task = COALESCE(p_current_task, current_task),
        urls_per_minute = COALESCE(p_urls_per_minute, urls_per_minute)
    WHERE server_id = p_server_id;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- FUNCTION: zen_register_server (Call when server starts)
-- ============================================================================

CREATE OR REPLACE FUNCTION zen_register_server(
    p_server_id VARCHAR(50),
    p_server_name VARCHAR(100),
    p_server_region VARCHAR(50),
    p_workers INTEGER,
    p_batch_size INTEGER
)
RETURNS VOID AS $$
DECLARE
    v_session_id VARCHAR(100);
BEGIN
    v_session_id := p_server_id || '_' || to_char(NOW(), 'YYYYMMDD_HH24MISS');

    INSERT INTO zen_servers (
        server_id, server_name, server_region, workers_count, batch_size,
        status, session_id, session_started, started_at, last_heartbeat
    ) VALUES (
        p_server_id, p_server_name, p_server_region, p_workers, p_batch_size,
        'online', v_session_id, NOW(), NOW(), NOW()
    )
    ON CONFLICT (server_id) DO UPDATE SET
        server_name = EXCLUDED.server_name,
        server_region = EXCLUDED.server_region,
        workers_count = EXCLUDED.workers_count,
        batch_size = EXCLUDED.batch_size,
        status = 'online',
        session_id = v_session_id,
        session_started = NOW(),
        session_processed = 0,
        session_errors = 0,
        last_heartbeat = NOW();
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- FUNCTION: zen_unregister_server (Call when server stops)
-- ============================================================================

CREATE OR REPLACE FUNCTION zen_unregister_server(p_server_id VARCHAR(50))
RETURNS VOID AS $$
BEGIN
    UPDATE zen_servers SET
        status = 'offline',
        current_task = NULL
    WHERE server_id = p_server_id;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- SAMPLE QUERIES FOR DASHBOARD
-- ============================================================================

-- Get main dashboard stats
-- SELECT * FROM zen_v_dashboard;

-- Get all server status
-- SELECT * FROM zen_v_server_status;

-- Get country progress
-- SELECT * FROM zen_v_country_progress ORDER BY pending DESC;

-- Get recent activity
-- SELECT * FROM zen_v_recent_activity LIMIT 100;

-- Get hourly stats for chart
-- SELECT * FROM zen_v_hourly_stats LIMIT 168;  -- Last 7 days

-- Get top countries by pending
-- SELECT country_code, pending, progress_percent
-- FROM zen_v_country_progress
-- WHERE pending > 0
-- ORDER BY pending DESC
-- LIMIT 20;


-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT 'Schema created successfully!' AS status;

-- Show all tables
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
AND table_name LIKE 'zen_%'
ORDER BY table_name;

-- Show all views
SELECT table_name AS view_name
FROM information_schema.views
WHERE table_schema = 'public'
AND table_name LIKE 'zen_%'
ORDER BY table_name;
