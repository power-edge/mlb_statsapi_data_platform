-- ============================================================================
-- MLB Stats API Data Platform - Schema Migration V4
-- ============================================================================
--
-- Purpose: Consolidate home/away tables and enhance with missing columns
--
-- Changes:
-- 1. Consolidate 8 home/away tables into 4 unified tables with team flags
--    - batting_order (home + away)
--    - bench (home + away)
--    - bullpen (home + away)
--    - pitchers (home + away)
--
-- 2. Add Statcast hitData columns to pitch_events table
--    - launch_speed (exit velocity)
--    - launch_angle
--    - total_distance
--    - trajectory
--    - hardness
--    - hit coordinates
--
-- 3. Add missing columns to other tables
--
-- ============================================================================

-- ============================================================================
-- PART 1: CONSOLIDATE HOME/AWAY TABLES
-- ============================================================================

-- Drop old separate home/away tables
DROP TABLE IF EXISTS game.live_game_home_batting_order CASCADE;
DROP TABLE IF EXISTS game.live_game_away_batting_order CASCADE;
DROP TABLE IF EXISTS game.live_game_home_bench CASCADE;
DROP TABLE IF EXISTS game.live_game_away_bench CASCADE;
DROP TABLE IF EXISTS game.live_game_home_bullpen CASCADE;
DROP TABLE IF EXISTS game.live_game_away_bullpen CASCADE;
DROP TABLE IF EXISTS game.live_game_home_pitchers CASCADE;
DROP TABLE IF EXISTS game.live_game_away_pitchers CASCADE;

-- Create consolidated batting_order table
-- Combines home and away batting orders with team flag
CREATE TABLE IF NOT EXISTS game.live_game_batting_order (
    game_pk BIGINT NOT NULL,
    team_id INT NOT NULL,
    is_home BOOLEAN NOT NULL,
    batting_position INT NOT NULL,
    player_id INT NOT NULL,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, team_id, batting_position)
);

CREATE INDEX IF NOT EXISTS idx_live_game_batting_order_pk ON game.live_game_batting_order (game_pk, team_id);
CREATE INDEX IF NOT EXISTS idx_live_game_batting_order_team ON game.live_game_batting_order (team_id);
COMMENT ON TABLE game.live_game_batting_order IS 'Starting batting order for both teams (consolidated home/away)';
COMMENT ON COLUMN game.live_game_batting_order.team_id IS 'Team ID (home_team_id or away_team_id from metadata)';
COMMENT ON COLUMN game.live_game_batting_order.is_home IS 'TRUE if home team, FALSE if away team';
COMMENT ON COLUMN game.live_game_batting_order.batting_position IS '1-9, position in batting order';

-- Create consolidated bench table
-- Combines home and away bench players with team flag
CREATE TABLE IF NOT EXISTS game.live_game_bench (
    game_pk BIGINT NOT NULL,
    team_id INT NOT NULL,
    is_home BOOLEAN NOT NULL,
    player_id INT NOT NULL,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, team_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_live_game_bench_pk ON game.live_game_bench (game_pk, team_id);
CREATE INDEX IF NOT EXISTS idx_live_game_bench_team ON game.live_game_bench (team_id);
COMMENT ON TABLE game.live_game_bench IS 'Bench players for both teams (consolidated home/away)';
COMMENT ON COLUMN game.live_game_bench.team_id IS 'Team ID (home_team_id or away_team_id from metadata)';
COMMENT ON COLUMN game.live_game_bench.is_home IS 'TRUE if home team, FALSE if away team';

-- Create consolidated bullpen table
-- Combines home and away bullpen with team flag
CREATE TABLE IF NOT EXISTS game.live_game_bullpen (
    game_pk BIGINT NOT NULL,
    team_id INT NOT NULL,
    is_home BOOLEAN NOT NULL,
    pitcher_id INT NOT NULL,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, team_id, pitcher_id)
);

CREATE INDEX IF NOT EXISTS idx_live_game_bullpen_pk ON game.live_game_bullpen (game_pk, team_id);
CREATE INDEX IF NOT EXISTS idx_live_game_bullpen_team ON game.live_game_bullpen (team_id);
COMMENT ON TABLE game.live_game_bullpen IS 'Available relief pitchers for both teams (consolidated home/away)';
COMMENT ON COLUMN game.live_game_bullpen.team_id IS 'Team ID (home_team_id or away_team_id from metadata)';
COMMENT ON COLUMN game.live_game_bullpen.is_home IS 'TRUE if home team, FALSE if away team';

-- Create consolidated pitchers table
-- Combines home and away pitchers used in game with team flag
CREATE TABLE IF NOT EXISTS game.live_game_pitchers (
    game_pk BIGINT NOT NULL,
    team_id INT NOT NULL,
    is_home BOOLEAN NOT NULL,
    pitcher_id INT NOT NULL,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, team_id, pitcher_id)
);

CREATE INDEX IF NOT EXISTS idx_live_game_pitchers_pk ON game.live_game_pitchers (game_pk, team_id);
CREATE INDEX IF NOT EXISTS idx_live_game_pitchers_team ON game.live_game_pitchers (team_id);
COMMENT ON TABLE game.live_game_pitchers IS 'Pitchers used in game for both teams (consolidated home/away)';
COMMENT ON COLUMN game.live_game_pitchers.team_id IS 'Team ID (home_team_id or away_team_id from metadata)';
COMMENT ON COLUMN game.live_game_pitchers.is_home IS 'TRUE if home team, FALSE if away team';

-- ============================================================================
-- PART 2: CREATE SEPARATE HIT_DATA TABLE FOR STATCAST BATTED BALL DATA
-- ============================================================================

-- Create hit_data table for Statcast batted ball tracking
-- Only populated when ball is put in play (is_in_play = true in pitch_events)
-- Links to pitch_events via play_id
CREATE TABLE IF NOT EXISTS game.live_game_hit_data (
    game_pk BIGINT NOT NULL,
    play_id VARCHAR(50) NOT NULL,           -- Links to pitch_events.play_id
    at_bat_index INT NOT NULL,              -- Links to plays.at_bat_index
    -- Statcast hit tracking
    launch_speed NUMERIC(5,2),              -- Exit velocity (MPH)
    launch_angle NUMERIC(5,2),              -- Launch angle (degrees)
    total_distance NUMERIC(6,2),            -- Distance traveled (feet)
    trajectory VARCHAR(50),                 -- ground_ball, line_drive, fly_ball, popup
    hardness VARCHAR(50),                   -- soft, medium, hard
    hit_location INT,                       -- Field location code (1-9)
    hit_coord_x NUMERIC(6,2),               -- Landing coordinates X
    hit_coord_y NUMERIC(6,2),               -- Landing coordinates Y
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, play_id)
);

CREATE INDEX IF NOT EXISTS idx_live_game_hit_data_game ON game.live_game_hit_data (game_pk);
CREATE INDEX IF NOT EXISTS idx_live_game_hit_data_at_bat ON game.live_game_hit_data (game_pk, at_bat_index);

-- Statcast analysis indexes
CREATE INDEX IF NOT EXISTS idx_live_game_hit_data_exit_velo ON game.live_game_hit_data (launch_speed)
    WHERE launch_speed IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_live_game_hit_data_barrel ON game.live_game_hit_data (launch_speed, launch_angle)
    WHERE launch_speed >= 98 AND launch_angle BETWEEN 26 AND 30;

COMMENT ON TABLE game.live_game_hit_data IS 'Statcast batted ball data - only populated when ball is put in play';
COMMENT ON COLUMN game.live_game_hit_data.play_id IS 'Links to pitch_events.play_id - the pitch that resulted in this batted ball';
COMMENT ON COLUMN game.live_game_hit_data.launch_speed IS 'Exit velocity in MPH (Statcast)';
COMMENT ON COLUMN game.live_game_hit_data.launch_angle IS 'Launch angle in degrees (Statcast)';
COMMENT ON COLUMN game.live_game_hit_data.total_distance IS 'Total distance traveled in feet';
COMMENT ON COLUMN game.live_game_hit_data.trajectory IS 'Ball trajectory: ground_ball, line_drive, fly_ball, popup';
COMMENT ON COLUMN game.live_game_hit_data.hardness IS 'Contact quality: soft, medium, hard';

-- ============================================================================
-- PART 3: ADD MISSING COLUMNS TO OTHER TABLES
-- ============================================================================

-- Add missing columns to metadata table
ALTER TABLE game.live_game_metadata
    ADD COLUMN IF NOT EXISTS home_hits INT,
    ADD COLUMN IF NOT EXISTS away_hits INT,
    ADD COLUMN IF NOT EXISTS home_errors INT,
    ADD COLUMN IF NOT EXISTS away_errors INT,
    ADD COLUMN IF NOT EXISTS current_inning INT,
    ADD COLUMN IF NOT EXISTS inning_state VARCHAR(20),
    ADD COLUMN IF NOT EXISTS inning_half VARCHAR(10),
    ADD COLUMN IF NOT EXISTS outs INT,
    ADD COLUMN IF NOT EXISTS balls INT,
    ADD COLUMN IF NOT EXISTS strikes INT;

COMMENT ON COLUMN game.live_game_metadata.current_inning IS 'Current inning (1-9+) - only populated for live games';
COMMENT ON COLUMN game.live_game_metadata.inning_state IS 'Top, Middle, Bottom, End';
COMMENT ON COLUMN game.live_game_metadata.inning_half IS 'Top or Bottom';

-- Add missing columns to venue_details table
ALTER TABLE game.live_game_venue_details
    ADD COLUMN IF NOT EXISTS state_abbrev VARCHAR(2),
    ADD COLUMN IF NOT EXISTS country_code VARCHAR(2),
    ADD COLUMN IF NOT EXISTS capacity INT,
    ADD COLUMN IF NOT EXISTS surface VARCHAR(50),
    ADD COLUMN IF NOT EXISTS roof_type VARCHAR(50),
    ADD COLUMN IF NOT EXISTS left_line NUMERIC(5,1),
    ADD COLUMN IF NOT EXISTS left_center NUMERIC(5,1),
    ADD COLUMN IF NOT EXISTS center NUMERIC(5,1),
    ADD COLUMN IF NOT EXISTS right_center NUMERIC(5,1),
    ADD COLUMN IF NOT EXISTS right_line NUMERIC(5,1);

COMMENT ON COLUMN game.live_game_venue_details.surface IS 'Grass, Turf, etc.';
COMMENT ON COLUMN game.live_game_venue_details.roof_type IS 'Open, Retractable, Dome';
COMMENT ON COLUMN game.live_game_venue_details.left_line IS 'Distance to left field line (feet)';
COMMENT ON COLUMN game.live_game_venue_details.center IS 'Distance to center field (feet)';
COMMENT ON COLUMN game.live_game_venue_details.right_line IS 'Distance to right field line (feet)';

-- Add missing columns to umpires table
ALTER TABLE game.live_game_umpires
    ADD COLUMN IF NOT EXISTS full_name VARCHAR(100);

COMMENT ON COLUMN game.live_game_umpires.full_name IS 'Umpire full name (same as umpire_name for consistency)';

-- Add missing columns to plays table
ALTER TABLE game.live_game_plays
    ADD COLUMN IF NOT EXISTS batter_name VARCHAR(100),
    ADD COLUMN IF NOT EXISTS pitcher_name VARCHAR(100),
    ADD COLUMN IF NOT EXISTS batter_team_id INT,
    ADD COLUMN IF NOT EXISTS pitcher_team_id INT,
    ADD COLUMN IF NOT EXISTS home_team_id INT,
    ADD COLUMN IF NOT EXISTS away_team_id INT,
    ADD COLUMN IF NOT EXISTS is_scoring_play BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_review BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_out BOOLEAN,
    ADD COLUMN IF NOT EXISTS event_code VARCHAR(50);

COMMENT ON COLUMN game.live_game_plays.batter_name IS 'Batter full name for convenience';
COMMENT ON COLUMN game.live_game_plays.pitcher_name IS 'Pitcher full name for convenience';
COMMENT ON COLUMN game.live_game_plays.is_scoring_play IS 'TRUE if this play resulted in runs';
COMMENT ON COLUMN game.live_game_plays.has_review IS 'TRUE if play was reviewed';
COMMENT ON COLUMN game.live_game_plays.has_out IS 'TRUE if play resulted in out(s)';

-- Add missing columns to play_actions table
ALTER TABLE game.live_game_play_actions
    ADD COLUMN IF NOT EXISTS is_pickoff BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_stolen_base BOOLEAN,
    ADD COLUMN IF NOT EXISTS runner_id INT;

COMMENT ON COLUMN game.live_game_play_actions.is_pickoff IS 'TRUE if action is pickoff attempt';
COMMENT ON COLUMN game.live_game_play_actions.is_stolen_base IS 'TRUE if action is stolen base';
COMMENT ON COLUMN game.live_game_play_actions.runner_id IS 'Runner involved in action';

-- Add missing columns to runners table
ALTER TABLE game.live_game_runners
    ADD COLUMN IF NOT EXISTS runner_name VARCHAR(100),
    ADD COLUMN IF NOT EXISTS is_stolen_base BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_wild_pitch BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_passed_ball BOOLEAN,
    ADD COLUMN IF NOT EXISTS runner_team_id INT;

COMMENT ON COLUMN game.live_game_runners.runner_name IS 'Runner full name for convenience';
COMMENT ON COLUMN game.live_game_runners.is_stolen_base IS 'TRUE if runner advanced on stolen base';
COMMENT ON COLUMN game.live_game_runners.is_wild_pitch IS 'TRUE if runner advanced on wild pitch';
COMMENT ON COLUMN game.live_game_runners.is_passed_ball IS 'TRUE if runner advanced on passed ball';

-- Add missing columns to scoring_plays table
ALTER TABLE game.live_game_scoring_plays
    ADD COLUMN IF NOT EXISTS event VARCHAR(100),
    ADD COLUMN IF NOT EXISTS rbi INT,
    ADD COLUMN IF NOT EXISTS play_id VARCHAR(50);

COMMENT ON COLUMN game.live_game_scoring_plays.event IS 'Event type (e.g., Home Run, Single)';
COMMENT ON COLUMN game.live_game_scoring_plays.rbi IS 'RBI on this play';
COMMENT ON COLUMN game.live_game_scoring_plays.play_id IS 'Reference to play_id in plays table';

-- ============================================================================
-- COMPLETION
-- ============================================================================

-- Log migration completion
INSERT INTO metadata.schema_versions (endpoint, method, version, schema_definition, notes)
VALUES (
    'game',
    'liveGameV1',
    'v2',
    '{}',
    'V4: Consolidated home/away tables; Created separate hit_data table for Statcast; Enhanced all tables with missing columns'
)
ON CONFLICT (endpoint, method, version) DO UPDATE
    SET notes = EXCLUDED.notes,
        schema_definition = EXCLUDED.schema_definition;

-- Summary
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'MLB Data Platform - Schema V4 Complete';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Consolidated tables: 4 (batting_order, bench, bullpen, pitchers)';
    RAISE NOTICE 'Removed tables: 8 (home/away variants)';
    RAISE NOTICE 'New table: live_game_hit_data (Statcast batted ball data)';
    RAISE NOTICE 'Enhanced other tables with: 30+ missing columns';
    RAISE NOTICE '========================================';
END $$;
