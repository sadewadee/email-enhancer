-- ============================================================================
-- FAILURE TRACKING MIGRATION
-- ============================================================================
-- Adds retry_count and failure tracking to zen_contacts for rows that were
-- claimed but not successfully enriched.
--
-- Author: Claude (Sonnet 4)
-- Date: 2025-12-01
-- ============================================================================

-- Add retry tracking columns if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'zen_contacts' AND column_name = 'retry_count') THEN
        ALTER TABLE zen_contacts ADD COLUMN retry_count INTEGER DEFAULT 0;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'zen_contacts' AND column_name = 'last_retry_at') THEN
        ALTER TABLE zen_contacts ADD COLUMN last_retry_at TIMESTAMPTZ;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'zen_contacts' AND column_name = 'claimed_at') THEN
        ALTER TABLE zen_contacts ADD COLUMN claimed_at TIMESTAMPTZ;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'zen_contacts' AND column_name = 'claimed_by') THEN
        ALTER TABLE zen_contacts ADD COLUMN claimed_by VARCHAR(50);
    END IF;
END $$;

-- Create index for retry queries
CREATE INDEX IF NOT EXISTS idx_zc_retry ON zen_contacts(retry_count) 
    WHERE scrape_status = 'failed' AND retry_count < 3;

-- Create index for stale claims (claimed but not completed after timeout)
CREATE INDEX IF NOT EXISTS idx_zc_claimed ON zen_contacts(claimed_at) 
    WHERE claimed_at IS NOT NULL AND scrape_status = 'pending';


-- ============================================================================
-- FUNCTION: zen_mark_failed (Mark row as failed, increment retry count)
-- ============================================================================

CREATE OR REPLACE FUNCTION zen_mark_failed(
    p_source_link VARCHAR(2048),
    p_error TEXT,
    p_server_id VARCHAR(50)
)
RETURNS VOID AS $$
DECLARE
    v_partition_key INTEGER;
BEGIN
    v_partition_key := ABS(hashtext(p_source_link)) % 32;
    
    UPDATE zen_contacts SET
        scrape_status = 'failed',
        scrape_error = p_error,
        retry_count = retry_count + 1,
        last_retry_at = NOW(),
        last_scrape_server = p_server_id,
        last_scrape_at = NOW(),
        claimed_at = NULL,
        claimed_by = NULL
    WHERE source_link = p_source_link
    AND partition_key = v_partition_key;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- FUNCTION: zen_claim_for_retry (Claim failed rows for retry)
-- ============================================================================

CREATE OR REPLACE FUNCTION zen_claim_for_retry(
    p_server_id VARCHAR(50),
    p_batch_size INTEGER DEFAULT 100,
    p_max_retries INTEGER DEFAULT 3,
    p_retry_delay_minutes INTEGER DEFAULT 30
)
RETURNS TABLE (
    source_link VARCHAR(2048),
    partition_key INTEGER,
    business_website VARCHAR(2048),
    retry_count INTEGER
) AS $$
BEGIN
    RETURN QUERY
    UPDATE zen_contacts zc SET
        claimed_at = NOW(),
        claimed_by = p_server_id,
        scrape_status = 'pending'
    FROM (
        SELECT z.source_link, z.partition_key
        FROM zen_contacts z
        WHERE z.scrape_status = 'failed'
        AND z.retry_count < p_max_retries
        AND (z.last_retry_at IS NULL OR z.last_retry_at < NOW() - (p_retry_delay_minutes || ' minutes')::INTERVAL)
        AND (z.claimed_at IS NULL OR z.claimed_at < NOW() - INTERVAL '30 minutes')
        AND pg_try_advisory_xact_lock(z.id)
        ORDER BY z.retry_count ASC, z.last_retry_at ASC NULLS FIRST
        LIMIT p_batch_size
    ) AS to_claim
    WHERE zc.source_link = to_claim.source_link
    AND zc.partition_key = to_claim.partition_key
    RETURNING zc.source_link, zc.partition_key, zc.business_website, zc.retry_count;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- FUNCTION: zen_release_stale_claims (Release claims that timed out)
-- ============================================================================

CREATE OR REPLACE FUNCTION zen_release_stale_claims(
    p_timeout_minutes INTEGER DEFAULT 30
)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    UPDATE zen_contacts SET
        claimed_at = NULL,
        claimed_by = NULL,
        scrape_status = 'failed',
        scrape_error = COALESCE(scrape_error, '') || ' [TIMEOUT: claim expired]',
        retry_count = retry_count + 1
    WHERE claimed_at IS NOT NULL
    AND claimed_at < NOW() - (p_timeout_minutes || ' minutes')::INTERVAL
    AND scrape_status = 'pending';
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- VIEW: zen_v_failure_stats (Failure statistics for monitoring)
-- ============================================================================

CREATE OR REPLACE VIEW zen_v_failure_stats AS
SELECT
    country_code,
    COUNT(*) FILTER (WHERE scrape_status = 'failed' AND retry_count = 0) AS failed_no_retry,
    COUNT(*) FILTER (WHERE scrape_status = 'failed' AND retry_count = 1) AS failed_retry_1,
    COUNT(*) FILTER (WHERE scrape_status = 'failed' AND retry_count = 2) AS failed_retry_2,
    COUNT(*) FILTER (WHERE scrape_status = 'failed' AND retry_count >= 3) AS failed_max_retries,
    COUNT(*) FILTER (WHERE claimed_at IS NOT NULL AND scrape_status = 'pending') AS currently_claimed,
    COUNT(*) FILTER (WHERE scrape_status = 'failed' AND retry_count < 3) AS eligible_for_retry
FROM zen_contacts
GROUP BY country_code
ORDER BY eligible_for_retry DESC;


-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT 'Failure tracking migration complete!' AS status;

-- Show new columns
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'zen_contacts'
AND column_name IN ('retry_count', 'last_retry_at', 'claimed_at', 'claimed_by');
