-- ============================================================================
-- Migration: Partitioned Schema for Multi-Country Scraping (165 countries)
-- ============================================================================
-- Scale: 100M+ rows
-- Strategy: HASH partitioning (32 partitions) + country/category indexes
-- 
-- Why HASH instead of LIST by country?
-- - 165 countries = 165 partitions = management overhead
-- - HASH distributes evenly regardless of country distribution
-- - Combined with country INDEX = fast filtered queries
--
-- Author: Claude (Sonnet 4)
-- Date: 2025-11-30
-- ============================================================================

-- ============================================================================
-- STEP 1: Create new partitioned table
-- ============================================================================

CREATE TABLE IF NOT EXISTS scraped_contacts_v2 (
    -- Primary Key (composite with partition key)
    id BIGSERIAL,
    
    -- Partition key (for even distribution)
    partition_key INTEGER NOT NULL DEFAULT 0,
    
    -- ========== BUSINESS IDENTITY ==========
    link VARCHAR(2048) NOT NULL,              -- Google Maps URL (UNIQUE per partition)
    country VARCHAR(2) NOT NULL DEFAULT 'XX', -- ISO 3166-1 alpha-2 (US, ID, SG, etc.)
    title VARCHAR(500),
    category VARCHAR(255),
    
    -- ========== LOCATION ==========
    address TEXT,
    complete_address JSONB,                   -- {city, state, postal_code, street}
    latitude NUMERIC(10, 8),
    longitude NUMERIC(11, 8),
    plus_code VARCHAR(50),
    timezone VARCHAR(100),
    
    -- ========== GOOGLE MAPS DATA ==========
    website VARCHAR(2048),
    phone VARCHAR(100),
    open_hours JSONB,
    popular_times JSONB,
    review_count INTEGER,
    review_rating NUMERIC(3, 2),
    reviews_per_rating JSONB,
    price_range VARCHAR(50),
    cid VARCHAR(255),
    data_id VARCHAR(255),
    status VARCHAR(50),
    descriptions TEXT,
    about TEXT,
    thumbnail VARCHAR(2048),
    images TEXT[],
    reservations VARCHAR(2048),
    order_online VARCHAR(2048),
    menu VARCHAR(2048),
    reviews_link VARCHAR(2048),
    owner JSONB,
    
    -- ========== ENRICHMENT: EMAILS ==========
    emails TEXT[],                            -- Array of emails found
    emails_validated JSONB,                   -- [{email, valid, reason, confidence}, ...]
    emails_count INTEGER DEFAULT 0,
    
    -- ========== ENRICHMENT: PHONES ==========  
    phones TEXT[],                            -- Array of phone numbers
    phones_validated JSONB,
    phones_count INTEGER DEFAULT 0,
    
    -- ========== ENRICHMENT: WHATSAPP ==========
    whatsapp TEXT[],                          -- Array of WhatsApp numbers
    whatsapp_validated JSONB,
    whatsapp_count INTEGER DEFAULT 0,
    
    -- ========== ENRICHMENT: SOCIAL MEDIA ==========
    facebook VARCHAR(500),
    instagram VARCHAR(500),
    linkedin VARCHAR(500),
    tiktok VARCHAR(500),
    youtube VARCHAR(500),
    twitter VARCHAR(500),
    
    -- ========== SCRAPING METADATA ==========
    source_id INTEGER,                        -- Reference to results.id
    final_url VARCHAR(2048),
    was_redirected BOOLEAN DEFAULT FALSE,
    scraping_status VARCHAR(50) DEFAULT 'pending',  -- pending, success, failed, skipped
    scraping_error TEXT,
    processing_time NUMERIC(10, 3),
    pages_scraped INTEGER DEFAULT 0,
    
    -- ========== AUDIT ==========
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    scrape_count INTEGER DEFAULT 0,
    last_scraped_by VARCHAR(50),              -- server_id that last processed
    
    -- ========== CONSTRAINTS ==========
    PRIMARY KEY (id, partition_key),
    UNIQUE (link, partition_key)
    
) PARTITION BY HASH (partition_key);

-- ============================================================================
-- STEP 2: Create 32 hash partitions (optimal for 100M+ rows)
-- ============================================================================

DO $$
BEGIN
    FOR i IN 0..31 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS scraped_contacts_v2_p%s PARTITION OF scraped_contacts_v2 FOR VALUES WITH (MODULUS 32, REMAINDER %s)',
            i, i
        );
    END LOOP;
END $$;

-- ============================================================================
-- STEP 3: Create indexes for common queries
-- ============================================================================

-- Country index (most common filter)
CREATE INDEX IF NOT EXISTS idx_sc_country ON scraped_contacts_v2(country);

-- Category index
CREATE INDEX IF NOT EXISTS idx_sc_category ON scraped_contacts_v2(category);

-- Composite: country + category (common query pattern)
CREATE INDEX IF NOT EXISTS idx_sc_country_category ON scraped_contacts_v2(country, category);

-- Scraping status (for finding pending/failed)
CREATE INDEX IF NOT EXISTS idx_sc_status ON scraped_contacts_v2(scraping_status);

-- Composite: country + status (for "pending in Indonesia")
CREATE INDEX IF NOT EXISTS idx_sc_country_status ON scraped_contacts_v2(country, scraping_status);

-- Link hash index (for UPSERT lookups)
CREATE INDEX IF NOT EXISTS idx_sc_link_hash ON scraped_contacts_v2 USING hash(link);

-- Updated timestamp (for "recently scraped")
CREATE INDEX IF NOT EXISTS idx_sc_updated ON scraped_contacts_v2(updated_at DESC);

-- Source ID (for joining back to results table)
CREATE INDEX IF NOT EXISTS idx_sc_source ON scraped_contacts_v2(source_id);

-- Email count > 0 (for "has emails" filter)
CREATE INDEX IF NOT EXISTS idx_sc_has_emails ON scraped_contacts_v2(emails_count) WHERE emails_count > 0;

-- ============================================================================
-- STEP 4: Create trigger for auto-update timestamp
-- ============================================================================

CREATE OR REPLACE FUNCTION update_scraped_contacts_v2_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sc_updated ON scraped_contacts_v2;
CREATE TRIGGER trg_sc_updated
    BEFORE UPDATE ON scraped_contacts_v2
    FOR EACH ROW
    EXECUTE FUNCTION update_scraped_contacts_v2_timestamp();

-- ============================================================================
-- STEP 5: Create function to calculate partition key from link
-- ============================================================================

CREATE OR REPLACE FUNCTION get_partition_key(link_url TEXT)
RETURNS INTEGER AS $$
BEGIN
    -- Use hashtext for consistent distribution
    RETURN ABS(hashtext(link_url)) % 32;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- STEP 6: Create UPSERT function for enrichment
-- ============================================================================

CREATE OR REPLACE FUNCTION upsert_enriched_contact(
    p_link VARCHAR(2048),
    p_country VARCHAR(2),
    p_title VARCHAR(500),
    p_category VARCHAR(255),
    p_source_id INTEGER,
    p_emails TEXT[],
    p_phones TEXT[],
    p_whatsapp TEXT[],
    p_facebook VARCHAR(500),
    p_instagram VARCHAR(500),
    p_tiktok VARCHAR(500),
    p_youtube VARCHAR(500),
    p_final_url VARCHAR(2048),
    p_was_redirected BOOLEAN,
    p_scraping_status VARCHAR(50),
    p_scraping_error TEXT,
    p_processing_time NUMERIC,
    p_pages_scraped INTEGER,
    p_server_id VARCHAR(50)
)
RETURNS VOID AS $$
DECLARE
    v_partition_key INTEGER;
BEGIN
    -- Calculate partition key
    v_partition_key := get_partition_key(p_link);
    
    -- UPSERT with conflict handling
    INSERT INTO scraped_contacts_v2 (
        link, partition_key, country, title, category, source_id,
        emails, emails_count,
        phones, phones_count,
        whatsapp, whatsapp_count,
        facebook, instagram, tiktok, youtube,
        final_url, was_redirected, scraping_status, scraping_error,
        processing_time, pages_scraped, last_scraped_by, scrape_count
    ) VALUES (
        p_link, v_partition_key, p_country, p_title, p_category, p_source_id,
        p_emails, COALESCE(array_length(p_emails, 1), 0),
        p_phones, COALESCE(array_length(p_phones, 1), 0),
        p_whatsapp, COALESCE(array_length(p_whatsapp, 1), 0),
        p_facebook, p_instagram, p_tiktok, p_youtube,
        p_final_url, p_was_redirected, p_scraping_status, p_scraping_error,
        p_processing_time, p_pages_scraped, p_server_id, 1
    )
    ON CONFLICT (link, partition_key) DO UPDATE SET
        -- Merge emails (union of old + new, deduplicated)
        emails = ARRAY(
            SELECT DISTINCT unnest 
            FROM unnest(
                COALESCE(scraped_contacts_v2.emails, '{}') || 
                COALESCE(EXCLUDED.emails, '{}')
            )
        ),
        emails_count = (
            SELECT COUNT(DISTINCT e) 
            FROM unnest(
                COALESCE(scraped_contacts_v2.emails, '{}') || 
                COALESCE(EXCLUDED.emails, '{}')
            ) AS e
        ),
        -- Merge phones
        phones = ARRAY(
            SELECT DISTINCT unnest 
            FROM unnest(
                COALESCE(scraped_contacts_v2.phones, '{}') || 
                COALESCE(EXCLUDED.phones, '{}')
            )
        ),
        phones_count = (
            SELECT COUNT(DISTINCT p) 
            FROM unnest(
                COALESCE(scraped_contacts_v2.phones, '{}') || 
                COALESCE(EXCLUDED.phones, '{}')
            ) AS p
        ),
        -- Merge WhatsApp
        whatsapp = ARRAY(
            SELECT DISTINCT unnest 
            FROM unnest(
                COALESCE(scraped_contacts_v2.whatsapp, '{}') || 
                COALESCE(EXCLUDED.whatsapp, '{}')
            )
        ),
        whatsapp_count = (
            SELECT COUNT(DISTINCT w) 
            FROM unnest(
                COALESCE(scraped_contacts_v2.whatsapp, '{}') || 
                COALESCE(EXCLUDED.whatsapp, '{}')
            ) AS w
        ),
        -- Social media: prefer new if not null
        facebook = COALESCE(EXCLUDED.facebook, scraped_contacts_v2.facebook),
        instagram = COALESCE(EXCLUDED.instagram, scraped_contacts_v2.instagram),
        tiktok = COALESCE(EXCLUDED.tiktok, scraped_contacts_v2.tiktok),
        youtube = COALESCE(EXCLUDED.youtube, scraped_contacts_v2.youtube),
        -- Metadata: always update
        final_url = EXCLUDED.final_url,
        was_redirected = EXCLUDED.was_redirected,
        scraping_status = EXCLUDED.scraping_status,
        scraping_error = EXCLUDED.scraping_error,
        processing_time = EXCLUDED.processing_time,
        pages_scraped = EXCLUDED.pages_scraped,
        last_scraped_by = EXCLUDED.last_scraped_by,
        scrape_count = scraped_contacts_v2.scrape_count + 1,
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- STEP 7: Create views for common queries
-- ============================================================================

-- View: Contacts with emails only
CREATE OR REPLACE VIEW v_contacts_with_emails AS
SELECT * FROM scraped_contacts_v2 WHERE emails_count > 0;

-- View: Contacts by country with stats
CREATE OR REPLACE VIEW v_country_stats AS
SELECT 
    country,
    COUNT(*) as total_contacts,
    SUM(emails_count) as total_emails,
    SUM(phones_count) as total_phones,
    SUM(whatsapp_count) as total_whatsapp,
    COUNT(*) FILTER (WHERE scraping_status = 'success') as successful,
    COUNT(*) FILTER (WHERE scraping_status = 'failed') as failed,
    COUNT(*) FILTER (WHERE scraping_status = 'pending') as pending
FROM scraped_contacts_v2
GROUP BY country
ORDER BY total_contacts DESC;

-- View: Recent scrapes
CREATE OR REPLACE VIEW v_recent_scrapes AS
SELECT 
    country, title, link, emails_count, phones_count,
    scraping_status, processing_time, last_scraped_by, updated_at
FROM scraped_contacts_v2
ORDER BY updated_at DESC
LIMIT 1000;

-- ============================================================================
-- STEP 8: Grant permissions (adjust user as needed)
-- ============================================================================

-- GRANT ALL ON scraped_contacts_v2 TO zenvoyer_db;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO zenvoyer_db;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check partitions created
SELECT 
    parent.relname AS parent,
    child.relname AS partition
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'scraped_contacts_v2'
ORDER BY child.relname;

-- Check indexes
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'scraped_contacts_v2';

-- Show table structure
\d scraped_contacts_v2

SELECT 'Migration completed successfully!' AS status;
