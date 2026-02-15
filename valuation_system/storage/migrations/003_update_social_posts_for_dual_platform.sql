-- Migration: Update vs_social_posts for dual-platform (Twitter + LinkedIn)
-- Date: 2026-02-14
-- Purpose: Add linkedin_content column and 'both' platform option

ALTER TABLE vs_social_posts
MODIFY COLUMN platform ENUM('twitter', 'linkedin', 'both') DEFAULT 'twitter';

ALTER TABLE vs_social_posts
ADD COLUMN linkedin_content TEXT NULL AFTER content;
