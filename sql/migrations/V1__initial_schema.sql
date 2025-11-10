-- V1: Initial MLB Data Platform Schema
-- Creates: metadata schema, season schema, schedule schema, game schema (raw tables only)
-- Run Date: 2024-11-09

-- ==============================================================================
-- METADATA SCHEMA
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS metadata;

-- Schema version tracking
CREATE TABLE metadata.schema_versions (
    id SERIAL PRIMARY KEY,
    endpoint VARCHAR(50) NOT NULL,
    method VARCHAR(50) NOT NULL,
    version VARCHAR(10) NOT NULL,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    schema_definition JSONB NOT NULL,  -- Full JSON schema from pymlb_statsapi
    breaking_changes TEXT[],
    added_fields TEXT[],
    deprecated_fields TEXT[],
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (endpoint, method, version)
);

CREATE INDEX idx_schema_versions_endpoint ON metadata.schema_versions(endpoint, method);
CREATE INDEX idx_schema_versions_effective ON metadata.schema_versions(effective_date DESC);

-- Job execution tracking
CREATE TABLE metadata.job_executions (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    job_type VARCHAR(20) NOT NULL,  -- batch, streaming, scheduled
    execution_id VARCHAR(100) UNIQUE NOT NULL,  -- Argo Workflow ID or UUID
    status VARCHAR(20) NOT NULL,  -- pending, running, completed, failed
    start_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    end_time TIMESTAMPTZ,
    duration_seconds INT,
    records_processed BIGINT,
    records_failed BIGINT,
    error_message TEXT,
    metadata JSONB,

    INDEX idx_job_executions_name (job_name, start_time DESC),
    INDEX idx_job_executions_status (status),
    INDEX idx_job_executions_execution_id (execution_id)
);

-- ==============================================================================
-- SEASON SCHEMA
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS season;

-- Raw season data (complete JSON from Season.seasons())
CREATE TABLE season.seasons (
    id BIGSERIAL PRIMARY KEY,

    -- Request metadata
    request_params JSONB NOT NULL,
    source_url TEXT NOT NULL,

    -- Response metadata
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    schema_version VARCHAR(10) NOT NULL DEFAULT 'v1',
    response_status INT NOT NULL DEFAULT 200,

    -- Raw data payload
    data JSONB NOT NULL,

    -- Extracted keys for indexing
    sport_id INT,
    season_id VARCHAR(10),

    -- Ingestion metadata
    ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
    ingestion_job_id VARCHAR(100),

    INDEX idx_seasons_captured_at (captured_at DESC),
    INDEX idx_seasons_sport_id (sport_id),
    INDEX idx_seasons_season_id (season_id)
);

-- Partition by captured_at (monthly)
-- Note: Partitions will be created dynamically by the application

COMMENT ON TABLE season.seasons IS 'Raw season data from Season.seasons() endpoint';

-- ==============================================================================
-- SCHEDULE SCHEMA
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS schedule;

-- Raw schedule data (complete JSON from Schedule.schedule())
CREATE TABLE schedule.schedule (
    id BIGSERIAL PRIMARY KEY,

    -- Request metadata
    request_params JSONB NOT NULL,
    source_url TEXT NOT NULL,

    -- Response metadata
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    schema_version VARCHAR(10) NOT NULL DEFAULT 'v1',
    response_status INT NOT NULL DEFAULT 200,

    -- Raw data payload
    data JSONB NOT NULL,

    -- Extracted keys for indexing/partitioning
    sport_id INT,
    schedule_date DATE NOT NULL,  -- The date this schedule is for
    game_date DATE,               -- Actual game date (may differ for postponements)
    season VARCHAR(10),

    -- Ingestion metadata
    ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
    ingestion_job_id VARCHAR(100),

    INDEX idx_schedule_captured_at (captured_at DESC),
    INDEX idx_schedule_schedule_date (schedule_date DESC),
    INDEX idx_schedule_season (season)
) PARTITION BY RANGE (schedule_date);

-- Create partitions for current year and next year
CREATE TABLE schedule.schedule_2024 PARTITION OF schedule.schedule
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE schedule.schedule_2025 PARTITION OF schedule.schedule
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

COMMENT ON TABLE schedule.schedule IS 'Raw schedule data from Schedule.schedule() endpoint';

-- ==============================================================================
-- GAME SCHEMA
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS game;

-- Raw live game data (complete JSON from Game.liveGameV1())
-- This is the MASTER table - contains everything about a game
CREATE TABLE game.live_game_v1 (
    id BIGSERIAL,

    -- Request metadata
    request_params JSONB NOT NULL,
    source_url TEXT NOT NULL,

    -- Response metadata
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    schema_version VARCHAR(10) NOT NULL DEFAULT 'v1',
    response_status INT NOT NULL DEFAULT 200,

    -- Raw data payload (MASSIVE - 5000+ lines of JSON)
    data JSONB NOT NULL,

    -- Extracted keys for indexing/partitioning
    game_pk BIGINT NOT NULL,
    game_date DATE NOT NULL,
    season VARCHAR(10),
    game_type VARCHAR(5),           -- R (regular), P (playoff), etc.
    game_state VARCHAR(20),         -- Live, Final, Preview, etc.
    home_team_id INT,
    away_team_id INT,
    venue_id INT,

    -- Optional timecode (for historical snapshots)
    timecode VARCHAR(20),           -- YYYYMMDD_HHMMSS format
    is_latest BOOLEAN DEFAULT TRUE, -- Only one row per game_pk should be TRUE

    -- Ingestion metadata
    ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
    ingestion_job_id VARCHAR(100),

    PRIMARY KEY (id, game_date),
    INDEX idx_live_game_game_pk (game_pk),
    INDEX idx_live_game_captured_at (captured_at DESC),
    INDEX idx_live_game_game_date (game_date DESC),
    INDEX idx_live_game_state (game_state),
    INDEX idx_live_game_latest (game_pk, is_latest) WHERE is_latest = TRUE,
    INDEX idx_live_game_season (season),

    -- GIN index for fast JSONB queries
    INDEX idx_live_game_data_gin ON game.live_game_v1 USING GIN (data)
) PARTITION BY RANGE (game_date);

-- Create partitions for current year and next year
CREATE TABLE game.live_game_v1_2024 PARTITION OF game.live_game_v1
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE game.live_game_v1_2025 PARTITION OF game.live_game_v1
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

COMMENT ON TABLE game.live_game_v1 IS 'Raw live game data from Game.liveGameV1() endpoint - MASTER table containing all game data';

-- Trigger to ensure only one latest row per game_pk
CREATE OR REPLACE FUNCTION game.update_latest_game()
RETURNS TRIGGER AS $$
BEGIN
    -- When inserting a new row with is_latest = TRUE,
    -- set all other rows for this game_pk to is_latest = FALSE
    IF NEW.is_latest = TRUE THEN
        UPDATE game.live_game_v1
        SET is_latest = FALSE
        WHERE game_pk = NEW.game_pk
          AND id != NEW.id
          AND is_latest = TRUE;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_latest_game
BEFORE INSERT OR UPDATE ON game.live_game_v1
FOR EACH ROW
WHEN (NEW.is_latest = TRUE)
EXECUTE FUNCTION game.update_latest_game();

-- ==============================================================================
-- MATERIALIZED VIEWS (for performance)
-- ==============================================================================

-- Latest game state for each game
CREATE MATERIALIZED VIEW game.latest_live AS
SELECT DISTINCT ON (game_pk)
    game_pk,
    game_date,
    season,
    game_type,
    game_state,
    home_team_id,
    away_team_id,
    venue_id,
    captured_at,
    data
FROM game.live_game_v1
WHERE is_latest = TRUE
ORDER BY game_pk, captured_at DESC;

CREATE UNIQUE INDEX ON game.latest_live (game_pk);
CREATE INDEX ON game.latest_live (game_date DESC);
CREATE INDEX ON game.latest_live (game_state);

COMMENT ON MATERIALIZED VIEW game.latest_live IS 'Latest state for each game - refresh after each ingestion';

-- Today's schedule summary
CREATE MATERIALIZED VIEW schedule.today AS
SELECT
    schedule_date,
    sport_id,
    season,
    captured_at,
    jsonb_array_length(data->'dates'->0->'games') as game_count,
    data
FROM schedule.schedule
WHERE schedule_date = CURRENT_DATE
ORDER BY captured_at DESC
LIMIT 1;

CREATE INDEX ON schedule.today (schedule_date);

COMMENT ON MATERIALIZED VIEW schedule.today IS 'Today''s schedule - refresh every 15 minutes';

-- ==============================================================================
-- REFRESH FUNCTIONS (call these after data loads)
-- ==============================================================================

CREATE OR REPLACE FUNCTION metadata.refresh_all_views()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY game.latest_live;
    REFRESH MATERIALIZED VIEW CONCURRENTLY schedule.today;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION metadata.refresh_all_views IS 'Refresh all materialized views - call after data ingestion';

-- ==============================================================================
-- HELPER FUNCTIONS
-- ==============================================================================

-- Extract specific JSON path from game data
CREATE OR REPLACE FUNCTION game.extract_jsonpath(
    p_game_pk BIGINT,
    p_jsonpath TEXT
) RETURNS JSONB AS $$
    SELECT jsonb_path_query(data, p_jsonpath::jsonpath)
    FROM game.live_game_v1
    WHERE game_pk = p_game_pk
      AND is_latest = TRUE
    LIMIT 1;
$$ LANGUAGE SQL;

COMMENT ON FUNCTION game.extract_jsonpath IS 'Extract specific JSON path from latest game data';

-- ==============================================================================
-- GRANTS (for multi-tenancy)
-- ==============================================================================

-- Public read-only access (revoke in production)
GRANT USAGE ON SCHEMA metadata, season, schedule, game TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA metadata, season, schedule, game TO PUBLIC;
GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA game, schedule TO PUBLIC;

-- ==============================================================================
-- COMPLETION
-- ==============================================================================

-- Log migration completion
INSERT INTO metadata.schema_versions (endpoint, method, version, schema_definition, notes)
VALUES
    ('metadata', 'system', 'v1', '{}', 'Initial metadata schema'),
    ('season', 'seasons', 'v1', '{}', 'Initial season schema'),
    ('schedule', 'schedule', 'v1', '{}', 'Initial schedule schema'),
    ('game', 'liveGameV1', 'v1', '{}', 'Initial game schema with raw table only');

-- Summary
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'MLB Data Platform - Schema V1 Complete';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Schemas created: metadata, season, schedule, game';
    RAISE NOTICE 'Tables created: 5 (including partitions)';
    RAISE NOTICE 'Materialized views: 2';
    RAISE NOTICE 'Functions: 3';
    RAISE NOTICE '========================================';
END $$;
