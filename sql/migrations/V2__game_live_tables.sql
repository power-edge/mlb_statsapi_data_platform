-- ============================================================================
-- MLB Stats API Data Platform - Schema Migration
-- ============================================================================
--
-- Endpoint: game
-- Method: live_game_v1
-- Version: 1.0
--
-- Generated from: config/schemas/mappings/game/live_game_v1.yaml
--
-- This migration creates tables for the game.live_game_v1 API endpoint.
-- Tables are partitioned by captured_at timestamp (monthly partitions).
--
-- ============================================================================

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS game;

-- Core game information: teams, venue, status, timing, weather
-- Table: game.live_game_metadata
-- Primary Key: (game_pk)
-- Upsert Keys: (game_pk, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_metadata (
    game_pk BIGINT NOT NULL,
    game_type VARCHAR(5),
    season VARCHAR(4),
    season_type VARCHAR(20),
    game_date DATE NOT NULL,
    game_datetime TIMESTAMPTZ,
    day_night VARCHAR(10),
    time_zone VARCHAR(50),
    venue_id INT,
    venue_name VARCHAR(100),
    home_team_id INT,
    home_team_name VARCHAR(100),
    away_team_id INT,
    away_team_name VARCHAR(100),
    abstract_game_state VARCHAR(20),
    coded_game_state VARCHAR(5),
    detailed_state VARCHAR(50),
    status_code VARCHAR(5),
    reason TEXT,
    home_score INT,
    away_score INT,
    weather_condition VARCHAR(50),
    weather_temp INT,
    weather_wind VARCHAR(50),
    source_raw_id BIGINT,
    source_captured_at TIMESTAMPTZ,
    transform_timestamp TIMESTAMPTZ,
    PRIMARY KEY (game_pk)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_metadata_game_date ON game.live_game_metadata (game_date);
COMMENT ON TABLE game.live_game_metadata IS 'Core game information: teams, venue, status, timing, weather';
COMMENT ON COLUMN game.live_game_metadata.game_pk IS 'Unique game identifier';
COMMENT ON COLUMN game.live_game_metadata.game_type IS 'R=Regular, P=Playoff, W=World Series';
COMMENT ON COLUMN game.live_game_metadata.abstract_game_state IS 'Preview, Live, Final';
COMMENT ON COLUMN game.live_game_metadata.source_raw_id IS 'FK to game.live_game_v1.id';

-- Inning-by-inning scoring breakdown
-- Table: game.live_game_linescore_innings
-- Primary Key: (game_pk, inning_number, half_inning)
-- Upsert Keys: (game_pk, inning_number, half_inning, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_linescore_innings (
    game_pk BIGINT NOT NULL,
    inning_number INT NOT NULL,
    half_inning VARCHAR(10),
    home_runs INT,
    away_runs INT,
    home_hits INT,
    away_hits INT,
    home_errors INT,
    away_errors INT,
    home_left_on_base INT,
    away_left_on_base INT,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, inning_number, half_inning)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, inning_number, half_inning, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_linescore_innings_pk ON game.live_game_linescore_innings (game_pk, inning_number, half_inning);
COMMENT ON TABLE game.live_game_linescore_innings IS 'Inning-by-inning scoring breakdown';
COMMENT ON COLUMN game.live_game_linescore_innings.half_inning IS 'Top or Bottom';

-- All players involved in the game (51+ per game)
-- Table: game.live_game_players
-- Primary Key: (game_pk, player_id)
-- Upsert Keys: (game_pk, player_id, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_players (
    game_pk BIGINT NOT NULL,
    player_id INT NOT NULL,
    full_name VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    primary_number VARCHAR(5),
    birth_date DATE,
    current_age INT,
    birth_city VARCHAR(100),
    birth_country VARCHAR(100),
    height VARCHAR(10),
    weight INT,
    bats VARCHAR(1),
    throws VARCHAR(1),
    primary_position_code VARCHAR(2),
    primary_position_name VARCHAR(50),
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, player_id)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, player_id, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_players_pk ON game.live_game_players (game_pk, player_id);
COMMENT ON TABLE game.live_game_players IS 'All players involved in the game (51+ per game)';

-- All plays in the game (at-bats, ~75 per game)
-- Table: game.live_game_plays
-- Primary Key: (game_pk, play_id)
-- Upsert Keys: (game_pk, play_id, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_plays (
    game_pk BIGINT NOT NULL,
    play_id VARCHAR(50) NOT NULL,
    at_bat_index INT,
    inning INT,
    half_inning VARCHAR(10),
    is_top_inning BOOLEAN,
    outs_before INT,
    outs_after INT,
    event_type VARCHAR(50),
    event VARCHAR(100),
    description TEXT,
    rbi INT,
    away_score INT,
    home_score INT,
    batter_id INT,
    pitcher_id INT,
    batter_side VARCHAR(1),
    pitcher_hand VARCHAR(1),
    balls INT,
    strikes INT,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, play_id)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, play_id, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_plays_pk ON game.live_game_plays (game_pk, play_id);
COMMENT ON TABLE game.live_game_plays IS 'All plays in the game (at-bats, ~75 per game)';

-- Every pitch thrown in the game (~250 per game) - PITCH EVENTS ONLY (isPitch=true)
-- Table: game.live_game_pitch_events
-- Primary Key: (game_pk, play_id, pitch_number)
-- Upsert Keys: (game_pk, play_id, pitch_number, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_pitch_events (
    game_pk BIGINT NOT NULL,
    play_id VARCHAR(50) NOT NULL,
    pitch_number INT NOT NULL,
    action_index INT,
    pitch_type_code VARCHAR(10),
    pitch_type_description VARCHAR(100),
    pitch_call_code VARCHAR(5),
    pitch_call_description VARCHAR(50),
    is_strike BOOLEAN,
    is_ball BOOLEAN,
    is_in_play BOOLEAN,
    start_speed NUMERIC(5,2),
    end_speed NUMERIC(5,2),
    strike_zone_top NUMERIC(5,2),
    strike_zone_bottom NUMERIC(5,2),
    coordinates_x NUMERIC(6,2),
    coordinates_y NUMERIC(6,2),
    break_angle NUMERIC(5,2),
    break_length NUMERIC(5,2),
    break_y NUMERIC(5,2),
    spin_rate INT,
    spin_direction INT,
    balls_before INT,
    strikes_before INT,
    outs INT,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, play_id, pitch_number)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, play_id, pitch_number, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_pitch_events_pk ON game.live_game_pitch_events (game_pk, play_id, pitch_number);
COMMENT ON TABLE game.live_game_pitch_events IS 'Every pitch thrown in the game (~250 per game) - PITCH EVENTS ONLY (isPitch=true)';
COMMENT ON COLUMN game.live_game_pitch_events.start_speed IS 'MPH';

-- Non-pitch events: substitutions, pickoffs, mound visits, reviews (~10-20 per game) - NON-PITCH EVENTS ONLY (isPitch=false)
-- Table: game.live_game_play_actions
-- Primary Key: (game_pk, play_id, action_index)
-- Upsert Keys: (game_pk, play_id, action_index, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_play_actions (
    game_pk BIGINT NOT NULL,
    play_id VARCHAR(50) NOT NULL,
    action_index INT NOT NULL,
    action_type VARCHAR(50),
    event_type VARCHAR(50),
    description TEXT,
    is_substitution BOOLEAN,
    player_out_id INT,
    player_in_id INT,
    position VARCHAR(10),
    batting_order INT,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    balls INT,
    strikes INT,
    outs INT,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, play_id, action_index)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, play_id, action_index, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_play_actions_pk ON game.live_game_play_actions (game_pk, play_id, action_index);
COMMENT ON TABLE game.live_game_play_actions IS 'Non-pitch events: substitutions, pickoffs, mound visits, reviews (~10-20 per game) - NON-PITCH EVENTS ONLY (isPitch=false)';
COMMENT ON COLUMN game.live_game_play_actions.action_index IS 'Index of action within play';
COMMENT ON COLUMN game.live_game_play_actions.action_type IS 'substitution, pickoff, caught_stealing, stolen_base, etc.';
COMMENT ON COLUMN game.live_game_play_actions.player_out_id IS 'Player being replaced (for substitutions)';
COMMENT ON COLUMN game.live_game_play_actions.player_in_id IS 'Player entering game (for substitutions)';
COMMENT ON COLUMN game.live_game_play_actions.position IS 'Position code for substitution';
COMMENT ON COLUMN game.live_game_play_actions.batting_order IS 'Batting order position';

-- Defensive credits for each play (putouts, assists, errors)
-- Table: game.live_game_fielding_credits
-- Primary Key: (game_pk, play_id, event_index, credit_index)
-- Upsert Keys: (game_pk, play_id, event_index, credit_index, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_fielding_credits (
    game_pk BIGINT NOT NULL,
    play_id VARCHAR(50) NOT NULL,
    event_index INT NOT NULL,
    credit_index INT NOT NULL,
    player_id INT NOT NULL,
    player_name VARCHAR(100),
    position_code VARCHAR(5),
    position_name VARCHAR(50),
    credit_type VARCHAR(20),
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, play_id, event_index, credit_index)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, play_id, event_index, credit_index, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_fielding_credits_pk ON game.live_game_fielding_credits (game_pk, play_id, event_index, credit_index);
COMMENT ON TABLE game.live_game_fielding_credits IS 'Defensive credits for each play (putouts, assists, errors)';
COMMENT ON COLUMN game.live_game_fielding_credits.event_index IS 'Index of event within play';
COMMENT ON COLUMN game.live_game_fielding_credits.credit_index IS 'Index of credit within event';
COMMENT ON COLUMN game.live_game_fielding_credits.position_code IS 'P, C, 1B, 2B, 3B, SS, LF, CF, RF';
COMMENT ON COLUMN game.live_game_fielding_credits.credit_type IS 'f_putout, f_assist, f_error, f_fielded_ball, etc.';

-- Base runners and their movements for each play
-- Table: game.live_game_runners
-- Primary Key: (game_pk, play_id, runner_index)
-- Upsert Keys: (game_pk, play_id, runner_index, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_runners (
    game_pk BIGINT NOT NULL,
    play_id VARCHAR(50) NOT NULL,
    runner_index INT NOT NULL,
    runner_id INT,
    start_base VARCHAR(5),
    end_base VARCHAR(5),
    is_out BOOLEAN,
    out_number INT,
    event_type VARCHAR(50),
    movement_reason VARCHAR(100),
    is_scoring_event BOOLEAN,
    rbi BOOLEAN,
    earned_run BOOLEAN,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, play_id, runner_index)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, play_id, runner_index, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_runners_pk ON game.live_game_runners (game_pk, play_id, runner_index);
COMMENT ON TABLE game.live_game_runners IS 'Base runners and their movements for each play';
COMMENT ON COLUMN game.live_game_runners.runner_index IS 'Index of runner (0-2 for bases)';
COMMENT ON COLUMN game.live_game_runners.start_base IS 'Base runner started on (1B, 2B, 3B, score)';

-- Plays that resulted in runs scored
-- Table: game.live_game_scoring_plays
-- Primary Key: (game_pk, play_index)
-- Upsert Keys: (game_pk, play_index, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_scoring_plays (
    game_pk BIGINT NOT NULL,
    play_index INT NOT NULL,
    inning INT,
    half_inning VARCHAR(10),
    description TEXT,
    runs_scored INT,
    away_score_after INT,
    home_score_after INT,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, play_index)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, play_index, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_scoring_plays_pk ON game.live_game_scoring_plays (game_pk, play_index);
COMMENT ON TABLE game.live_game_scoring_plays IS 'Plays that resulted in runs scored';
COMMENT ON COLUMN game.live_game_scoring_plays.play_index IS 'Index from scoringPlays array';

-- Home team batting order
-- Table: game.live_game_home_batting_order
-- Primary Key: (game_pk, batting_position)
-- Upsert Keys: (game_pk, batting_position, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_home_batting_order (
    game_pk BIGINT NOT NULL,
    batting_position INT NOT NULL,
    player_id INT NOT NULL,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, batting_position)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, batting_position, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_home_batting_order_pk ON game.live_game_home_batting_order (game_pk, batting_position);
COMMENT ON TABLE game.live_game_home_batting_order IS 'Home team batting order';
COMMENT ON COLUMN game.live_game_home_batting_order.player_id IS 'Player ID from battingOrder array element';

-- Away team batting order
-- Table: game.live_game_away_batting_order
-- Primary Key: (game_pk, batting_position)
-- Upsert Keys: (game_pk, batting_position, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_away_batting_order (
    game_pk BIGINT NOT NULL,
    batting_position INT NOT NULL,
    player_id INT NOT NULL,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, batting_position)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, batting_position, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_away_batting_order_pk ON game.live_game_away_batting_order (game_pk, batting_position);
COMMENT ON TABLE game.live_game_away_batting_order IS 'Away team batting order';

-- Home team pitchers used in game
-- Table: game.live_game_home_pitchers
-- Primary Key: (game_pk, pitcher_id)
-- Upsert Keys: (game_pk, pitcher_id, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_home_pitchers (
    game_pk BIGINT NOT NULL,
    pitcher_id INT NOT NULL,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, pitcher_id)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, pitcher_id, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_home_pitchers_pk ON game.live_game_home_pitchers (game_pk, pitcher_id);
COMMENT ON TABLE game.live_game_home_pitchers IS 'Home team pitchers used in game';

-- Away team pitchers used in game
-- Table: game.live_game_away_pitchers
-- Primary Key: (game_pk, pitcher_id)
-- Upsert Keys: (game_pk, pitcher_id, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_away_pitchers (
    game_pk BIGINT NOT NULL,
    pitcher_id INT NOT NULL,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, pitcher_id)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, pitcher_id, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_away_pitchers_pk ON game.live_game_away_pitchers (game_pk, pitcher_id);
COMMENT ON TABLE game.live_game_away_pitchers IS 'Away team pitchers used in game';

-- Home team bench players
-- Table: game.live_game_home_bench
-- Primary Key: (game_pk, player_id)
-- Upsert Keys: (game_pk, player_id, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_home_bench (
    game_pk BIGINT NOT NULL,
    player_id INT NOT NULL,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, player_id)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, player_id, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_home_bench_pk ON game.live_game_home_bench (game_pk, player_id);
COMMENT ON TABLE game.live_game_home_bench IS 'Home team bench players';

-- Away team bench players
-- Table: game.live_game_away_bench
-- Primary Key: (game_pk, player_id)
-- Upsert Keys: (game_pk, player_id, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_away_bench (
    game_pk BIGINT NOT NULL,
    player_id INT NOT NULL,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, player_id)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, player_id, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_away_bench_pk ON game.live_game_away_bench (game_pk, player_id);
COMMENT ON TABLE game.live_game_away_bench IS 'Away team bench players';

-- Home team bullpen (available relief pitchers)
-- Table: game.live_game_home_bullpen
-- Primary Key: (game_pk, pitcher_id)
-- Upsert Keys: (game_pk, pitcher_id, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_home_bullpen (
    game_pk BIGINT NOT NULL,
    pitcher_id INT NOT NULL,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, pitcher_id)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, pitcher_id, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_home_bullpen_pk ON game.live_game_home_bullpen (game_pk, pitcher_id);
COMMENT ON TABLE game.live_game_home_bullpen IS 'Home team bullpen (available relief pitchers)';

-- Away team bullpen (available relief pitchers)
-- Table: game.live_game_away_bullpen
-- Primary Key: (game_pk, pitcher_id)
-- Upsert Keys: (game_pk, pitcher_id, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_away_bullpen (
    game_pk BIGINT NOT NULL,
    pitcher_id INT NOT NULL,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, pitcher_id)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, pitcher_id, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_away_bullpen_pk ON game.live_game_away_bullpen (game_pk, pitcher_id);
COMMENT ON TABLE game.live_game_away_bullpen IS 'Away team bullpen (available relief pitchers)';

-- Umpiring crew for the game
-- Table: game.live_game_umpires
-- Primary Key: (game_pk, umpire_position)
-- Upsert Keys: (game_pk, umpire_position, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_umpires (
    game_pk BIGINT NOT NULL,
    umpire_id INT NOT NULL,
    umpire_name VARCHAR(100),
    umpire_position VARCHAR(20) NOT NULL,
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk, umpire_position)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, umpire_position, game_date)

CREATE INDEX IF NOT EXISTS idx_live_game_umpires_pk ON game.live_game_umpires (game_pk, umpire_position);
COMMENT ON TABLE game.live_game_umpires IS 'Umpiring crew for the game';
COMMENT ON COLUMN game.live_game_umpires.umpire_position IS 'Home Plate, First Base, Second Base, Third Base';

-- Detailed venue information for the game
-- Table: game.live_game_venue_details
-- Primary Key: (game_pk)
-- Upsert Keys: (game_pk, source_captured_at)
CREATE TABLE IF NOT EXISTS game.live_game_venue_details (
    game_pk BIGINT NOT NULL,
    venue_id INT NOT NULL,
    venue_name VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    timezone_id VARCHAR(50),
    timezone_offset INT,
    field_info JSONB,
    is_active BOOLEAN,
    season VARCHAR(4),
    source_captured_at TIMESTAMPTZ,
    PRIMARY KEY (game_pk)
);
-- TODO: Add partitioning by game_date when performance requires it
-- Note: Requires adding game_date to primary key: (game_pk, game_date)

COMMENT ON TABLE game.live_game_venue_details IS 'Detailed venue information for the game';
COMMENT ON COLUMN game.live_game_venue_details.timezone_offset IS 'Offset from UTC in hours';
COMMENT ON COLUMN game.live_game_venue_details.field_info IS 'Field dimensions, surface type, roof type, etc.';

-- End of migration

