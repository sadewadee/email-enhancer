-- PostgreSQL CREATE TABLE script for Email Enhancer
-- Creates 'scraped_contacts' table with all 51 columns (35 existing + 16 new)
--
-- This script is for creating the table from scratch in an empty database.
-- Based on user's existing schema with 35 columns + email-enhancer enrichment.
--
-- Author: Claude (Sonnet 4.5)
-- Date: 2025-11-30

-- Connect to the database (comment out if running via Python)
-- \c zenvoyer_db

CREATE TABLE IF NOT EXISTS scraped_contacts (
    -- Primary Key (auto-increment)
    id BIGSERIAL PRIMARY KEY,

    -- Original 35 columns from Google Maps scraper
    input_id VARCHAR(255),
    link VARCHAR(2048) NOT NULL UNIQUE,  -- URL - UNIQUE constraint for UPSERT
    title VARCHAR(500),
    category VARCHAR(255),
    address TEXT,
    open_hours TEXT,
    popular_times TEXT,
    website VARCHAR(2048),
    phone VARCHAR(100),
    plus_code VARCHAR(50),
    review_count INTEGER,
    review_rating NUMERIC(3, 2),
    reviews_per_rating TEXT,
    latitude NUMERIC(10, 8),
    longitude NUMERIC(11, 8),
    cid VARCHAR(255),
    status VARCHAR(50),
    descriptions TEXT,
    reviews_link VARCHAR(2048),
    thumbnail VARCHAR(2048),
    timezone VARCHAR(100),
    price_range VARCHAR(50),
    data_id VARCHAR(255),
    images TEXT,
    reservations VARCHAR(2048),
    order_online VARCHAR(2048),
    menu VARCHAR(2048),
    owner VARCHAR(500),
    complete_address TEXT,
    about TEXT,
    user_reviews TEXT,
    user_reviews_extended TEXT,
    emails TEXT,  -- Original: might be TEXT or TEXT[]
    facebook VARCHAR(500),
    instagram VARCHAR(500),
    linkedin VARCHAR(500),
    whatsapp TEXT,  -- Original: might be TEXT or TEXT[]

    -- NEW columns from email-enhancer (16 columns)
    phones TEXT[],  -- Array of scraped phone numbers
    tiktok VARCHAR(500),
    youtube VARCHAR(500),
    validated_emails JSONB,  -- Email validation metadata
    validated_whatsapp JSONB,  -- WhatsApp validation metadata
    final_url VARCHAR(2048),  -- URL after redirects
    was_redirected BOOLEAN DEFAULT FALSE,
    scraping_status VARCHAR(50),  -- 'success', 'failed', 'no_contacts_found'
    scraping_error TEXT,
    processing_time NUMERIC(10, 3),  -- Seconds with millisecond precision
    pages_scraped INTEGER,
    emails_found INTEGER DEFAULT 0,
    phones_found INTEGER DEFAULT 0,
    whatsapp_found INTEGER DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    scrape_count INTEGER DEFAULT 1,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance

-- Hash index on 'link' for O(1) UPSERT lookup
CREATE INDEX IF NOT EXISTS idx_link_hash ON scraped_contacts USING hash(link);

-- B-tree indexes for filtering and sorting
CREATE INDEX IF NOT EXISTS idx_scraping_status ON scraped_contacts(scraping_status);
CREATE INDEX IF NOT EXISTS idx_updated_at ON scraped_contacts(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_created_at ON scraped_contacts(created_at DESC);

-- Optional: GIN indexes for array searches (uncomment if needed)
-- CREATE INDEX IF NOT EXISTS idx_emails_gin ON scraped_contacts USING gin(emails);
-- CREATE INDEX IF NOT EXISTS idx_phones_gin ON scraped_contacts USING gin(phones);
-- CREATE INDEX IF NOT EXISTS idx_whatsapp_gin ON scraped_contacts USING gin(whatsapp);

-- Create trigger function for auto-updating updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop existing trigger if it exists (to avoid error on re-run)
DROP TRIGGER IF EXISTS update_scraped_contacts_updated_at ON scraped_contacts;

-- Create trigger to auto-update updated_at on every UPDATE
CREATE TRIGGER update_scraped_contacts_updated_at
    BEFORE UPDATE ON scraped_contacts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Verify table creation
SELECT
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'scraped_contacts'
ORDER BY ordinal_position;

-- Display success message
SELECT
    'Table created successfully!' AS status,
    COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_name = 'scraped_contacts';
