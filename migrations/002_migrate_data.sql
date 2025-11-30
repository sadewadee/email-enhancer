-- ============================================================================
-- Migration: Transfer data from old table to new partitioned table
-- ============================================================================
-- Run this AFTER 001_partitioned_schema.sql
-- 
-- This migrates existing data from scraped_contacts to scraped_contacts_v2
-- ============================================================================

-- ============================================================================
-- STEP 1: Migrate existing scraped_contacts data
-- ============================================================================

INSERT INTO scraped_contacts_v2 (
    link, partition_key, country, title, category,
    address, latitude, longitude, plus_code, timezone,
    website, phone, review_count, review_rating, price_range,
    cid, data_id, status, descriptions, about, thumbnail,
    emails, emails_count,
    phones, phones_count,
    whatsapp, whatsapp_count,
    facebook, instagram, linkedin, tiktok, youtube,
    final_url, was_redirected, scraping_status, scraping_error,
    processing_time, pages_scraped, created_at, updated_at, scrape_count
)
SELECT 
    link,
    get_partition_key(link) as partition_key,
    COALESCE(
        -- Try to extract country from complete_address JSON
        CASE 
            WHEN complete_address IS NOT NULL AND complete_address != '' 
            THEN UPPER(LEFT(complete_address::json->>'country', 2))
            ELSE 'XX'
        END,
        'XX'
    ) as country,
    title,
    category,
    address,
    latitude,
    longitude,
    plus_code,
    timezone,
    website,
    phone,
    review_count,
    review_rating,
    price_range,
    cid,
    data_id,
    status,
    descriptions,
    about,
    thumbnail,
    -- Convert TEXT to TEXT[] for emails
    CASE 
        WHEN emails IS NOT NULL AND emails != '' 
        THEN string_to_array(emails, ';')
        ELSE NULL
    END as emails,
    COALESCE(emails_found, 0) as emails_count,
    -- Phones already array
    phones,
    COALESCE(phones_found, 0) as phones_count,
    -- Convert TEXT to TEXT[] for whatsapp
    CASE 
        WHEN whatsapp IS NOT NULL AND whatsapp != '' 
        THEN string_to_array(whatsapp, ';')
        ELSE NULL
    END as whatsapp,
    COALESCE(whatsapp_found, 0) as whatsapp_count,
    facebook,
    instagram,
    linkedin,
    tiktok,
    youtube,
    final_url,
    COALESCE(was_redirected, FALSE),
    COALESCE(scraping_status, 'unknown'),
    scraping_error,
    processing_time,
    pages_scraped,
    COALESCE(created_at, NOW()),
    COALESCE(updated_at, NOW()),
    COALESCE(scrape_count, 1)
FROM scraped_contacts
ON CONFLICT (link, partition_key) DO NOTHING;

-- ============================================================================
-- STEP 2: Verify migration
-- ============================================================================

SELECT 
    'Old table' as source,
    COUNT(*) as row_count 
FROM scraped_contacts
UNION ALL
SELECT 
    'New table' as source,
    COUNT(*) as row_count 
FROM scraped_contacts_v2;

-- ============================================================================
-- STEP 3: Show country distribution in new table
-- ============================================================================

SELECT country, COUNT(*) as count
FROM scraped_contacts_v2
GROUP BY country
ORDER BY count DESC
LIMIT 20;

SELECT 'Data migration completed!' AS status;
