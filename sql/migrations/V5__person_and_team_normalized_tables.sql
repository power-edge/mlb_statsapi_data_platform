-- ============================================================================
-- MLB Stats API Data Platform - Schema Migration V5
-- ============================================================================
--
-- Purpose: Create normalized tables for Person and Team transformations
--
-- Tables Created:
-- 1. person.person_info - Core biographical and position information
-- 2. team.team_info - Core team information and affiliations
--
-- ============================================================================

-- ============================================================================
-- PART 1: PERSON NORMALIZED TABLE
-- ============================================================================

-- Create person schema if not exists
CREATE SCHEMA IF NOT EXISTS person;

-- Create normalized person table
-- Named 'person.person' to match Person.person() method (single output table)
CREATE TABLE IF NOT EXISTS person.person (
    -- Foreign keys to raw table
    raw_person_id BIGINT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    source_url TEXT,

    -- Person identification
    person_id INT NOT NULL,
    full_name VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    primary_number VARCHAR(10),
    name_slug VARCHAR(100),

    -- Biographical info
    birth_date DATE,
    current_age INT,
    birth_city VARCHAR(100),
    birth_state_province VARCHAR(100),
    birth_country VARCHAR(100),
    height VARCHAR(20),  -- e.g., "6' 3\""
    weight INT,
    gender VARCHAR(10),

    -- Status flags
    active BOOLEAN,
    is_player BOOLEAN,
    is_verified BOOLEAN,

    -- Names
    use_name VARCHAR(50),
    use_last_name VARCHAR(50),
    boxscore_name VARCHAR(50),
    nick_name VARCHAR(50),
    pronunciation VARCHAR(100),

    -- Career info
    mlb_debut_date DATE,

    -- Primary position
    primary_position_code VARCHAR(10),
    primary_position_name VARCHAR(50),
    primary_position_type VARCHAR(50),
    primary_position_abbrev VARCHAR(10),

    -- Bat/pitch handedness
    bat_side_code VARCHAR(5),
    bat_side_desc VARCHAR(20),
    pitch_hand_code VARCHAR(5),
    pitch_hand_desc VARCHAR(20),

    -- Strike zone
    strike_zone_top DOUBLE PRECISION,
    strike_zone_bottom DOUBLE PRECISION,

    -- Composite primary key for versioning
    PRIMARY KEY (person_id, captured_at)
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_person_person_id ON person.person(person_id);
CREATE INDEX IF NOT EXISTS idx_person_full_name ON person.person(full_name);
CREATE INDEX IF NOT EXISTS idx_person_active ON person.person(active);
CREATE INDEX IF NOT EXISTS idx_person_captured_at ON person.person(captured_at);
CREATE INDEX IF NOT EXISTS idx_person_position ON person.person(primary_position_code);

-- ============================================================================
-- PART 2: TEAM NORMALIZED TABLE
-- ============================================================================

-- Create team schema if not exists
CREATE SCHEMA IF NOT EXISTS team;

-- Create normalized team table
-- Named 'team.team' to match Team.teams() method (single output table)
CREATE TABLE IF NOT EXISTS team.team (
    -- Foreign keys to raw table
    raw_team_id INT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    source_url TEXT,

    -- Team identification
    team_id INT NOT NULL,
    name VARCHAR(100),
    abbreviation VARCHAR(10),
    team_name VARCHAR(50),
    team_code VARCHAR(10),
    file_code VARCHAR(10),

    -- Location info
    location_name VARCHAR(100),
    short_name VARCHAR(50),
    franchise_name VARCHAR(100),
    club_name VARCHAR(50),

    -- Season and status
    season INT,
    first_year_of_play VARCHAR(10),
    active BOOLEAN,
    all_star_status VARCHAR(10),

    -- Venue
    venue_id INT,
    venue_name VARCHAR(100),

    -- Spring venue
    spring_venue_id INT,

    -- Spring league
    spring_league_id INT,
    spring_league_name VARCHAR(100),
    spring_league_abbrev VARCHAR(10),

    -- League
    league_id INT,
    league_name VARCHAR(100),

    -- Division
    division_id INT,
    division_name VARCHAR(100),

    -- Sport
    sport_id INT,
    sport_name VARCHAR(100),

    -- Composite primary key for versioning
    PRIMARY KEY (team_id, captured_at)
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_team_team_id ON team.team(team_id);
CREATE INDEX IF NOT EXISTS idx_team_name ON team.team(name);
CREATE INDEX IF NOT EXISTS idx_team_abbreviation ON team.team(abbreviation);
CREATE INDEX IF NOT EXISTS idx_team_active ON team.team(active);
CREATE INDEX IF NOT EXISTS idx_team_captured_at ON team.team(captured_at);
CREATE INDEX IF NOT EXISTS idx_team_league ON team.team(league_id);
CREATE INDEX IF NOT EXISTS idx_team_division ON team.team(division_id);
CREATE INDEX IF NOT EXISTS idx_team_sport ON team.team(sport_id);

-- ============================================================================
-- PART 3: SCHEDULE NORMALIZED TABLES (if not exists)
-- ============================================================================

-- Create schedule.schedule_metadata table if it doesn't exist
CREATE TABLE IF NOT EXISTS schedule.schedule_metadata (
    raw_id BIGINT NOT NULL,
    sport_id INT,
    schedule_date DATE,
    captured_at TIMESTAMPTZ,
    source_url TEXT,
    total_games INT,
    total_events INT,
    total_items INT,
    num_dates INT,
    PRIMARY KEY (raw_id, captured_at)
);

-- Create schedule.games table if it doesn't exist
CREATE TABLE IF NOT EXISTS schedule.games (
    raw_id BIGINT NOT NULL,
    sport_id INT,
    schedule_date DATE,
    captured_at TIMESTAMPTZ,
    source_url TEXT,
    game_date DATE,
    game_pk INT NOT NULL,
    game_guid VARCHAR(50),
    game_type VARCHAR(10),
    season VARCHAR(10),
    game_datetime TIMESTAMPTZ,
    official_date DATE,
    status_code VARCHAR(10),
    detailed_state VARCHAR(50),
    abstract_game_state VARCHAR(20),
    coded_game_state VARCHAR(10),
    away_team_id INT,
    away_team_name VARCHAR(100),
    away_wins INT,
    away_losses INT,
    away_score INT,
    home_team_id INT,
    home_team_name VARCHAR(100),
    home_wins INT,
    home_losses INT,
    home_score INT,
    venue_id INT,
    venue_name VARCHAR(100),
    double_header VARCHAR(10),
    day_night VARCHAR(10),
    scheduled_innings INT,
    games_in_series INT,
    series_game_number INT,
    description TEXT,
    game_number INT,
    if_necessary VARCHAR(10),
    tiebreaker VARCHAR(10),
    PRIMARY KEY (game_pk, captured_at)
);

-- Create indexes for schedule tables
CREATE INDEX IF NOT EXISTS idx_schedule_games_game_pk ON schedule.games(game_pk);
CREATE INDEX IF NOT EXISTS idx_schedule_games_game_date ON schedule.games(game_date);
CREATE INDEX IF NOT EXISTS idx_schedule_games_home_team ON schedule.games(home_team_id);
CREATE INDEX IF NOT EXISTS idx_schedule_games_away_team ON schedule.games(away_team_id);

-- ============================================================================
-- PART 4: SEASON NORMALIZED TABLE (if not exists)
-- ============================================================================

-- Create season schema if not exists
CREATE SCHEMA IF NOT EXISTS season;

-- Create season.seasons table (normalized output)
-- Raw data is in season.seasons_raw
CREATE TABLE IF NOT EXISTS season.seasons (
    raw_id BIGINT NOT NULL,
    sport_id INT,
    captured_at TIMESTAMPTZ,
    source_url TEXT,
    season_id VARCHAR(10),
    regular_season_start_date DATE,
    regular_season_end_date DATE,
    season_start_date DATE,
    season_end_date DATE,
    spring_start_date DATE,
    spring_end_date DATE,
    has_wildcard BOOLEAN,
    all_star_date DATE,
    game_level_gameday_type VARCHAR(50),
    season_level_gameday_type VARCHAR(50),
    qualifier_plate_appearances DOUBLE PRECISION,
    qualifier_outs_pitched DOUBLE PRECISION,
    PRIMARY KEY (season_id, sport_id, captured_at)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_seasons_season_id ON season.seasons(season_id);
CREATE INDEX IF NOT EXISTS idx_seasons_sport_id ON season.seasons(sport_id);

-- ============================================================================
-- DONE
-- ============================================================================
