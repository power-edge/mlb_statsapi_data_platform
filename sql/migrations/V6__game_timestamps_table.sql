-- V6: Game timestamps table for live game polling
--
-- Stores timestamps from Game.liveTimestampv11 endpoint
-- Used for:
-- - Tracking available game states for replay
-- - Determining which timestamps to fetch during backfill
-- - Monitoring live game progression

-- ============================================================================
-- GAME TIMESTAMPS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS game.live_game_timestamps (
    -- Primary key
    game_pk BIGINT NOT NULL,
    timecode_raw VARCHAR(20) NOT NULL,  -- YYYYMMDD_HHMMSS format

    -- Parsed timestamp for easier querying (populated by trigger)
    timecode_parsed TIMESTAMPTZ,

    -- When we captured this timestamp list
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Ingestion metadata
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (game_pk, timecode_raw)
);

-- Function to parse timecode on insert
CREATE OR REPLACE FUNCTION game.parse_timecode()
RETURNS TRIGGER AS $$
BEGIN
    -- Parse YYYYMMDD_HHMMSS format
    NEW.timecode_parsed := TO_TIMESTAMP(NEW.timecode_raw, 'YYYYMMDD_HH24MISS');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-populate parsed timestamp
CREATE TRIGGER trg_parse_timecode
    BEFORE INSERT OR UPDATE ON game.live_game_timestamps
    FOR EACH ROW
    EXECUTE FUNCTION game.parse_timecode();

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_game_timestamps_game_pk
    ON game.live_game_timestamps (game_pk);

CREATE INDEX IF NOT EXISTS idx_game_timestamps_parsed
    ON game.live_game_timestamps (timecode_parsed);

CREATE INDEX IF NOT EXISTS idx_game_timestamps_captured
    ON game.live_game_timestamps (captured_at DESC);

COMMENT ON TABLE game.live_game_timestamps IS
    'Available timestamps for each game from liveTimestampv11 endpoint';
COMMENT ON COLUMN game.live_game_timestamps.timecode_raw IS
    'Raw timestamp string in YYYYMMDD_HHMMSS format';
COMMENT ON COLUMN game.live_game_timestamps.timecode_parsed IS
    'Parsed timestamp for querying (auto-generated)';

-- ============================================================================
-- RAW TABLE FOR TIMESTAMP RESPONSES
-- ============================================================================

-- Raw storage for the full liveTimestampv11 response
CREATE TABLE IF NOT EXISTS game.live_game_timestamps_raw (
    game_pk BIGINT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,

    -- Full API response (array of timestamp strings)
    data JSONB NOT NULL,

    -- Request metadata
    endpoint VARCHAR(50) NOT NULL DEFAULT 'game',
    method VARCHAR(100) NOT NULL DEFAULT 'liveTimestampv11',
    params JSONB,
    url TEXT,
    status_code INT,

    -- Ingestion tracking
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Number of timestamps in response (for quick reference)
    timestamp_count INT GENERATED ALWAYS AS (
        jsonb_array_length(data)
    ) STORED,

    PRIMARY KEY (game_pk, captured_at)
);

CREATE INDEX IF NOT EXISTS idx_game_timestamps_raw_captured
    ON game.live_game_timestamps_raw (captured_at DESC);

COMMENT ON TABLE game.live_game_timestamps_raw IS
    'Raw API responses from Game.liveTimestampv11 - append-only for tracking';

-- ============================================================================
-- VIEW: Latest timestamps per game
-- ============================================================================

CREATE OR REPLACE VIEW game.latest_game_timestamps AS
SELECT DISTINCT ON (game_pk)
    game_pk,
    captured_at,
    timestamp_count,
    data
FROM game.live_game_timestamps_raw
ORDER BY game_pk, captured_at DESC;

COMMENT ON VIEW game.latest_game_timestamps IS
    'Most recent timestamp list for each game';

-- ============================================================================
-- FUNCTION: Parse and insert timestamps from raw response
-- ============================================================================

CREATE OR REPLACE FUNCTION game.insert_timestamps_from_raw()
RETURNS TRIGGER AS $$
BEGIN
    -- Insert each timestamp from the array
    INSERT INTO game.live_game_timestamps (game_pk, timecode_raw, captured_at)
    SELECT
        NEW.game_pk,
        jsonb_array_elements_text(NEW.data),
        NEW.captured_at
    ON CONFLICT (game_pk, timecode_raw) DO NOTHING;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-populate timestamps table from raw
DROP TRIGGER IF EXISTS trg_insert_timestamps ON game.live_game_timestamps_raw;
CREATE TRIGGER trg_insert_timestamps
    AFTER INSERT ON game.live_game_timestamps_raw
    FOR EACH ROW
    EXECUTE FUNCTION game.insert_timestamps_from_raw();

COMMENT ON FUNCTION game.insert_timestamps_from_raw() IS
    'Trigger function to parse raw timestamp array into normalized table';
