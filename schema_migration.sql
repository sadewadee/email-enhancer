-- PostgreSQL Schema Migration for Email Enhancer
-- Adds new columns to existing 'scraped_contacts' table in zenvoyer_db
--
-- Existing columns (35 total):
-- [input_id, link, title, category, address, open_hours, popular_times,
--  website, phone, plus_code, review_count, review_rating, reviews_per_rating,
--  latitude, longitude, cid, status, descriptions, reviews_link, thumbnail,
--  timezone, price_range, data_id, images, reservations, order_online, menu,
--  owner, complete_address, about, user_reviews, user_reviews_extended,
--  emails, facebook, instagram, linkedin, whatsapp]
--
-- This migration adds columns for web scraping enrichment (emails, phones,
-- social media, validation metadata, scraping status).
--
-- Author: Claude (Sonnet 4.5)
-- Date: 2025-11-30

-- Connect to the database
\c zenvoyer_db

-- Add new columns (IF NOT EXISTS to make migration idempotent)

-- Contact information
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS phones TEXT[];

-- Social media (TikTok and YouTube are new, others already exist)
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS tiktok VARCHAR(500);
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS youtube VARCHAR(500);

-- Validation metadata (JSONB for flexible schema)
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS validated_emails JSONB;
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS validated_whatsapp JSONB;

-- URL tracking
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS final_url VARCHAR(2048);
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS was_redirected BOOLEAN DEFAULT FALSE;

-- Scraping metadata
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS scraping_status VARCHAR(50);
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS scraping_error TEXT;
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS processing_time NUMERIC(10, 3);
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS pages_scraped INTEGER;

-- Extraction counts
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS emails_found INTEGER DEFAULT 0;
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS phones_found INTEGER DEFAULT 0;
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS whatsapp_found INTEGER DEFAULT 0;

-- Audit trail
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE scraped_contacts ADD COLUMN IF NOT EXISTS scrape_count INTEGER DEFAULT 1;

-- Create indexes for performance (IF NOT EXISTS to avoid errors on re-run)

-- Hash index on 'link' for fast UPSERT lookup (O(1) equality search)
CREATE INDEX IF NOT EXISTS idx_link_hash ON scraped_contacts USING hash(link);

-- B-tree index on scraping_status for filtering
CREATE INDEX IF NOT EXISTS idx_scraping_status ON scraped_contacts(scraping_status);

-- B-tree index on updated_at for time-based queries
CREATE INDEX IF NOT EXISTS idx_updated_at ON scraped_contacts(updated_at DESC);

-- GIN indexes for array searches (optional, uncomment if needed)
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

-- Verify migration
SELECT
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'scraped_contacts'
ORDER BY ordinal_position;

-- Check if 'link' column has UNIQUE constraint (required for UPSERT)
SELECT
    conname AS constraint_name,
    contype AS constraint_type
FROM pg_constraint
WHERE conrelid = 'scraped_contacts'::regclass
  AND contype IN ('p', 'u');  -- p = primary key, u = unique

-- Migration complete!
-- Next steps:
-- 1. Copy .env.example to .env
-- 2. Fill in database credentials
-- 3. Run: python main.py single test.csv --workers 2 --export-db
