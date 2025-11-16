-- V3: Raw data tables for full replay capability
--
-- Design:
-- - PostgreSQL as single source of truth
-- - Full API responses stored as JSONB
-- - Metadata from pymlb_statsapi (endpoint, method, params, url, status_code, captured_at)
-- - Versioning via composite primary key (entity_id, captured_at)
-- - Append-only for full replay capability
-- - Partitioned by captured_at for performance

-- ============================================================================
-- CREATE SCHEMAS FIRST
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS schedule;
CREATE SCHEMA IF NOT EXISTS season;
CREATE SCHEMA IF NOT EXISTS person;
CREATE SCHEMA IF NOT EXISTS team;
CREATE SCHEMA IF NOT EXISTS meta;

-- ============================================================================
-- GAME ENDPOINT RAW TABLES
-- ============================================================================

-- Raw table for Game.liveGameV1
CREATE TABLE IF NOT EXISTS game.live_game_v1_raw (
    -- Primary key: entity + version (captured_at)
    game_pk BIGINT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,

    -- Full API response (complete JSONB for replay)
    data JSONB NOT NULL,

    -- Request metadata (from pymlb_statsapi)
    endpoint VARCHAR(50) NOT NULL DEFAULT 'game',
    method VARCHAR(100) NOT NULL DEFAULT 'liveGameV1',
    params JSONB,
    url TEXT,
    status_code INT,

    -- Ingestion tracking
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (game_pk, captured_at)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_live_game_v1_raw_captured_at
    ON game.live_game_v1_raw (captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_live_game_v1_raw_ingested_at
    ON game.live_game_v1_raw (ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_live_game_v1_raw_status
    ON game.live_game_v1_raw (status_code);

-- Index for JSON queries (e.g., filter by game status)
CREATE INDEX IF NOT EXISTS idx_live_game_v1_raw_game_state
    ON game.live_game_v1_raw USING GIN ((data->'gameData'->'status'->'abstractGameState'));

COMMENT ON TABLE game.live_game_v1_raw IS 'Raw API responses from Game.liveGameV1 - append-only for full replay capability';
COMMENT ON COLUMN game.live_game_v1_raw.captured_at IS 'Timestamp when API request was made (from pymlb_statsapi)';
COMMENT ON COLUMN game.live_game_v1_raw.data IS 'Complete API response as JSONB';
COMMENT ON COLUMN game.live_game_v1_raw.ingested_at IS 'Timestamp when data was inserted into this table';

-- ============================================================================
-- SCHEDULE ENDPOINT RAW TABLES
-- ============================================================================

-- Raw table for Schedule.schedule
CREATE TABLE IF NOT EXISTS schedule.schedule_raw (
    -- Primary key: date + captured_at (schedule can be fetched multiple times per day)
    schedule_date DATE NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,

    -- Full API response
    data JSONB NOT NULL,

    -- Request metadata
    endpoint VARCHAR(50) NOT NULL DEFAULT 'schedule',
    method VARCHAR(100) NOT NULL DEFAULT 'schedule',
    params JSONB,
    url TEXT,
    status_code INT,

    -- Ingestion tracking
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (schedule_date, captured_at)
);

CREATE INDEX IF NOT EXISTS idx_schedule_raw_captured_at
    ON schedule.schedule_raw (captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_schedule_raw_ingested_at
    ON schedule.schedule_raw (ingested_at DESC);

-- Index for finding games in schedule
CREATE INDEX IF NOT EXISTS idx_schedule_raw_dates
    ON schedule.schedule_raw USING GIN ((data->'dates'));

COMMENT ON TABLE schedule.schedule_raw IS 'Raw API responses from Schedule.schedule - append-only for full replay';

-- ============================================================================
-- SEASON ENDPOINT RAW TABLES
-- ============================================================================

-- Raw table for Season.seasons
CREATE TABLE IF NOT EXISTS season.seasons_raw (
    -- Primary key: sport_id + captured_at
    sport_id INT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,

    -- Full API response
    data JSONB NOT NULL,

    -- Request metadata
    endpoint VARCHAR(50) NOT NULL DEFAULT 'season',
    method VARCHAR(100) NOT NULL DEFAULT 'seasons',
    params JSONB,
    url TEXT,
    status_code INT,

    -- Ingestion tracking
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (sport_id, captured_at)
);

CREATE INDEX IF NOT EXISTS idx_seasons_raw_captured_at
    ON season.seasons_raw (captured_at DESC);

COMMENT ON TABLE season.seasons_raw IS 'Raw API responses from Season.seasons - append-only for full replay';

-- ============================================================================
-- PERSON ENDPOINT RAW TABLES
-- ============================================================================

-- Raw table for Person.person
CREATE TABLE IF NOT EXISTS person.person_raw (
    -- Primary key: person_id + captured_at (players can be fetched multiple times)
    person_id BIGINT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,

    -- Full API response
    data JSONB NOT NULL,

    -- Request metadata
    endpoint VARCHAR(50) NOT NULL DEFAULT 'person',
    method VARCHAR(100) NOT NULL DEFAULT 'person',
    params JSONB,
    url TEXT,
    status_code INT,

    -- Ingestion tracking
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (person_id, captured_at)
);

CREATE INDEX IF NOT EXISTS idx_person_raw_captured_at
    ON person.person_raw (captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_person_raw_ingested_at
    ON person.person_raw (ingested_at DESC);

COMMENT ON TABLE person.person_raw IS 'Raw API responses from Person.person - append-only for full replay';

-- ============================================================================
-- TEAM ENDPOINT RAW TABLES
-- ============================================================================

-- Raw table for Team.team
CREATE TABLE IF NOT EXISTS team.team_raw (
    -- Primary key: team_id + captured_at
    team_id INT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,

    -- Full API response
    data JSONB NOT NULL,

    -- Request metadata
    endpoint VARCHAR(50) NOT NULL DEFAULT 'team',
    method VARCHAR(100) NOT NULL DEFAULT 'team',
    params JSONB,
    url TEXT,
    status_code INT,

    -- Ingestion tracking
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (team_id, captured_at)
);

CREATE INDEX IF NOT EXISTS idx_team_raw_captured_at
    ON team.team_raw (captured_at DESC);

COMMENT ON TABLE team.team_raw IS 'Raw API responses from Team.team - append-only for full replay';

-- ============================================================================
-- HELPER VIEWS FOR LATEST DATA
-- ============================================================================

-- View for latest raw game data per game_pk
CREATE OR REPLACE VIEW game.live_game_v1_raw_latest AS
SELECT DISTINCT ON (game_pk)
    game_pk,
    captured_at,
    data,
    endpoint,
    method,
    params,
    url,
    status_code,
    ingested_at
FROM game.live_game_v1_raw
ORDER BY game_pk, captured_at DESC;

COMMENT ON VIEW game.live_game_v1_raw_latest IS 'Latest raw game data per game_pk - useful for incremental processing';

-- View for latest schedule per date
CREATE OR REPLACE VIEW schedule.schedule_raw_latest AS
SELECT DISTINCT ON (schedule_date)
    schedule_date,
    captured_at,
    data,
    endpoint,
    method,
    params,
    url,
    status_code,
    ingested_at
FROM schedule.schedule_raw
ORDER BY schedule_date, captured_at DESC;

COMMENT ON VIEW schedule.schedule_raw_latest IS 'Latest schedule per date';

-- ============================================================================
-- CHECKPOINT TABLE FOR TRANSFORMATION TRACKING
-- ============================================================================

CREATE TABLE IF NOT EXISTS meta.transformation_checkpoints (
    transformation_name VARCHAR(100) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    last_processed_at TIMESTAMPTZ NOT NULL,
    last_processed_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (transformation_name, source_table)
);

COMMENT ON TABLE meta.transformation_checkpoints IS 'Track last processed timestamp for incremental transformations';

-- Example checkpoint entries
INSERT INTO meta.transformation_checkpoints (transformation_name, source_table, last_processed_at, last_processed_count)
VALUES
    ('flatten_live_game', 'game.live_game_v1_raw', '1970-01-01 00:00:00+00', 0),
    ('flatten_schedule', 'schedule.schedule_raw', '1970-01-01 00:00:00+00', 0),
    ('flatten_season', 'season.seasons_raw', '1970-01-01 00:00:00+00', 0),
    ('flatten_person', 'person.person_raw', '1970-01-01 00:00:00+00', 0),
    ('flatten_team', 'team.team_raw', '1970-01-01 00:00:00+00', 0)
ON CONFLICT (transformation_name, source_table) DO NOTHING;
