-- Migration 053: Seed reference.news_source with PoC feed list
--
-- Date: 2026-05-06
--
-- Why:
--   The Market Field news collector needs registered sources to know
--   what to fetch. Starting with 5 free feeds chosen to cover the four
--   topic categories without paid subscriptions:
--
--     biofuels_digest  RSS     veg_oil_demand, policy_industry, competitor_activity
--     agweek           RSS     weather, soybean_supply, competitor_activity
--     brownfield_ag    RSS     meal_livestock_demand, soybean_supply, weather
--     epa_news         RSS     policy_federal, veg_oil_demand (RFS, biofuel rules)
--     gdelt            API     broad ag/biofuel coverage (noise floor)
--
--   These are PoC sources — once the dynamics work we'll layer paid
--   feeds (Reuters via NewsAPI Business, etc.) on top by adding new
--   rows here. URL templates should work as of seed date but RSS feeds
--   move; the collector logs failures and surfaces dead feeds.
--
--   default_topic_focus is a HINT for the classifier when a source has
--   strong topic bias (e.g., BiofuelsDigest is unlikely to publish
--   pure-livestock stories). Classifier still examines the article
--   text — it just gets a prior.
--
--   source_weight scales the intensity of a story's exogenous-forcing
--   contribution. 1.0 = full weight; lower = less trusted.

INSERT INTO reference.news_source
    (source_name, source_type, url_template, polling_frequency_minutes,
     default_locality, default_topic_focus, source_weight, is_active)
VALUES
    ('biofuels_digest', 'rss',
     'https://biofuelsdigest.com/feed/',
     360,    -- 6 hours
     'national',
     ARRAY['veg_oil_demand', 'policy_industry', 'policy_federal', 'competitor_activity'],
     1.0, TRUE),

    ('agweek', 'rss',
     'https://www.agweek.com/index.rss',
     360,
     'regional',
     ARRAY['weather', 'soybean_supply', 'competitor_activity', 'meal_livestock_demand'],
     1.0, TRUE),

    ('brownfield_ag', 'rss',
     'https://www.brownfieldagnews.com/feed/',
     360,
     'regional',
     ARRAY['meal_livestock_demand', 'soybean_supply', 'weather'],
     0.9, TRUE),

    -- Deactivated 2026-05-06 — kept in seed for traceability:
    -- EPA: https://www.epa.gov/newsreleases/search/rss returns 202 with empty
    --      body; no working RSS endpoint found across alternative URLs.
    -- GDELT: rate-limit (1 req per 5s) blocks PoC. Revisit with proper
    --        exponential backoff once dynamics validated.
    ('epa_news', 'rss',
     'https://www.epa.gov/newsreleases/search/rss',
     720,
     'national',
     ARRAY['policy_federal', 'veg_oil_demand'],
     1.0, FALSE),

    ('gdelt', 'gdelt',
     'https://api.gdeltproject.org/api/v2/doc/doc?query={query}&mode=ArtList&format=json',
     180,
     'national',
     ARRAY[]::TEXT[],
     0.6, FALSE)

ON CONFLICT (source_name) DO UPDATE SET
    source_type = EXCLUDED.source_type,
    url_template = EXCLUDED.url_template,
    polling_frequency_minutes = EXCLUDED.polling_frequency_minutes,
    default_locality = EXCLUDED.default_locality,
    default_topic_focus = EXCLUDED.default_topic_focus,
    source_weight = EXCLUDED.source_weight,
    is_active = EXCLUDED.is_active;
