-- Migration: Add search_query column to vs_event_timeline
-- Date: 2026-02-14
-- Purpose: Track which search query/keyword captured each news event for debugging duplicates

ALTER TABLE vs_event_timeline
ADD COLUMN search_query VARCHAR(200) DEFAULT NULL
COMMENT 'Search term/keyword that captured this news (for dedup debugging)'
AFTER source_url;

-- Add index for query analysis
CREATE INDEX idx_search_query ON vs_event_timeline(search_query);
