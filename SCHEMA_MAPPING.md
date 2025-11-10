# MLB StatsAPI Data Platform - Schema Mapping

This document defines the **complete mapping** from `pymlb_statsapi` endpoint/method structure to PostgreSQL database schemas and tables.

## Core Mapping Principle

```
pymlb_statsapi Endpoint.method() → PostgreSQL schema.table_name
```

### Naming Convention

1. **Schema name** = endpoint name (lowercase)
2. **Raw table name** = method name (snake_case)
3. **Normalized tables** = raw_table_name + descriptive suffix

---

## Layer 1: RAW Data Layer

Raw layer stores the **complete JSON response** from each API endpoint with minimal transformation.

### Table Structure (All Raw Tables)

```sql
CREATE TABLE schema.method_name (
    id BIGSERIAL PRIMARY KEY,

    -- API request metadata
    request_params JSONB NOT NULL,      -- All parameters used in the request
    source_url TEXT NOT NULL,            -- Full API URL called

    -- Response metadata
    captured_at TIMESTAMPTZ NOT NULL,    -- When the API call was made
    schema_version VARCHAR(10) NOT NULL, -- API schema version (v1, v2, etc.)
    response_status INT NOT NULL,        -- HTTP status code

    -- Raw data payload
    data JSONB NOT NULL,                 -- Complete JSON response

    -- Partitioning/indexing keys (extracted from data for performance)
    game_pk BIGINT,                      -- For game-related tables
    game_date DATE,                      -- For date-based partitioning
    season VARCHAR(4),                   -- For season-based queries

    -- Ingestion metadata
    ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
    ingestion_job_id VARCHAR(100),       -- Argo Workflow job ID

    -- Indexes
    INDEX idx_captured_at (captured_at),
    INDEX idx_game_pk (game_pk) WHERE game_pk IS NOT NULL,
    INDEX idx_game_date (game_date) WHERE game_date IS NOT NULL
) PARTITION BY RANGE (captured_at);
```

---

## Complete Endpoint/Method → Schema.Table Mapping

### 1. Schedule Endpoint

| Endpoint.Method | PostgreSQL Schema.Table | Description |
|----------------|-------------------------|-------------|
| `Schedule.schedule()` | `schedule.schedule` | Daily schedule for a sport/date |
| `Schedule.tieGames()` | `schedule.tie_games` | Tied games in a season |
| `Schedule.postseason()` | `schedule.postseason` | Postseason schedule |
| `Schedule.postseasonSeries()` | `schedule.postseason_series` | Specific postseason series |
| `Schedule.postseasonTuneIn()` | `schedule.postseason_tune_in` | Postseason broadcast info |

**Key Relationships:**
- `schedule.schedule` → contains `games[]` array
- Each game references `game.live_game_v1` via `game_pk`

---

### 2. Game Endpoint

| Endpoint.Method | PostgreSQL Schema.Table | Description |
|----------------|-------------------------|-------------|
| `Game.liveGameV1()` | `game.live_game_v1` | **MASTER** - Complete live game feed |
| `Game.boxscore()` | `game.boxscore` | Traditional boxscore data |
| `Game.linescore()` | `game.linescore` | Inning-by-inning line score |
| `Game.playByPlay()` | `game.play_by_play` | Play-by-play feed |
| `Game.content()` | `game.content` | Media content (highlights, videos) |
| `Game.winProbability()` | `game.win_probability` | Win probability by play |
| `Game.contextMetrics()` | `game.context_metrics` | Context metrics (leverage, WPA) |
| `Game.getGamePaceLogs()` | `game.pace_logs` | Game pace analytics |

**Special Note on `game.live_game_v1`:**

This is the **most comprehensive** game data source containing:
- Game metadata
- All players (51+ players per game)
- All plays (75+ plays per game)
- All pitches (200+ pitches per game)
- Live scoring, lineups, substitutions

---

### 3. Team Endpoint

| Endpoint.Method | PostgreSQL Schema.Table | Description |
|----------------|-------------------------|-------------|
| `Team.teams()` | `team.teams` | All teams for a sport |
| `Team.team()` | `team.team` | Single team details |
| `Team.roster()` | `team.roster` | Team roster by date |
| `Team.rosterDepthCharts()` | `team.roster_depth_charts` | Depth chart by position |
| `Team.alumni()` | `team.alumni` | Former players |
| `Team.coaches()` | `team.coaches` | Coaching staff |
| `Team.personnel()` | `team.personnel` | All team personnel |
| `Team.affiliates()` | `team.affiliates` | Minor league affiliates |

---

### 4. Person (Player) Endpoint

| Endpoint.Method | PostgreSQL Schema.Table | Description |
|----------------|-------------------------|-------------|
| `Person.person()` | `person.person` | Player biographical data |
| `Person.stats()` | `person.stats` | Player career/season stats |
| `Person.statstreaks()` | `person.stat_streaks` | Current hot/cold streaks |

---

### 5. Season Endpoint

| Endpoint.Method | PostgreSQL Schema.Table | Description |
|----------------|-------------------------|-------------|
| `Season.seasons()` | `season.seasons` | All seasons for a sport |
| `Season.season()` | `season.season` | Single season details |

---

### 6. Stats Endpoint

| Endpoint.Method | PostgreSQL Schema.Table | Description |
|----------------|-------------------------|-------------|
| `Stats.stats()` | `stats.stats` | Statistical data (batting, pitching, fielding) |
| `Stats.leaders()` | `stats.leaders` | League leaders |

---

### 7. Venue Endpoint

| Endpoint.Method | PostgreSQL Schema.Table | Description |
|----------------|-------------------------|-------------|
| `Venue.venues()` | `venue.venues` | All venues |
| `Venue.venue()` | `venue.venue` | Single venue details |

---

### 8. Additional Endpoints

| Endpoint | PostgreSQL Schema | Number of Methods |
|----------|-------------------|-------------------|
| `Draft` | `draft` | 5 methods |
| `Division` | `division` | 2 methods |
| `League` | `league` | 3 methods |
| `Sports` | `sports` | 2 methods |
| `Awards` | `awards` | 3 methods |
| `HighLow` | `high_low` | 2 methods |
| `Standings` | `standings` | 1 method |

---

## Layer 2: NORMALIZED Data Layer

Normalized layer **extracts and flattens** nested JSON structures from raw tables into relational tables with proper foreign keys.

### Example: `game.live_game_v1` Decomposition

The `game.live_game_v1` raw table contains a massive nested structure. We extract it into:

#### Top-Level Game Metadata
```sql
CREATE TABLE game.live_game_metadata (
    id BIGSERIAL PRIMARY KEY,
    raw_id BIGINT REFERENCES game.live_game_v1(id),

    -- Core identifiers
    game_pk BIGINT UNIQUE NOT NULL,
    game_type VARCHAR(5),
    season VARCHAR(4),
    game_date DATE,

    -- Status
    abstract_game_state VARCHAR(20),
    coded_game_state VARCHAR(5),
    detailed_state VARCHAR(50),
    status_code VARCHAR(5),

    -- Timing
    game_datetime TIMESTAMPTZ,
    official_date DATE,
    day_night VARCHAR(10),

    -- Teams
    home_team_id INT,
    away_team_id INT,
    home_score INT,
    away_score INT,

    -- Venue
    venue_id INT,
    venue_name VARCHAR(100),

    -- Game info
    attendance INT,
    first_pitch TIMESTAMPTZ,
    game_duration_minutes INT,

    -- Weather
    weather_condition VARCHAR(100),
    weather_temp VARCHAR(10),
    weather_wind VARCHAR(50),

    -- Flags
    is_no_hitter BOOLEAN,
    is_perfect_game BOOLEAN,

    INDEX idx_game_pk (game_pk),
    INDEX idx_game_date (game_date),
    INDEX idx_season (season)
);
```

#### Players (from gameData.players object)
```sql
CREATE TABLE game.live_game_players (
    id BIGSERIAL PRIMARY KEY,
    raw_id BIGINT REFERENCES game.live_game_v1(id),
    game_pk BIGINT NOT NULL,

    -- Player identification
    player_id INT NOT NULL,
    full_name VARCHAR(100),
    link TEXT,

    -- Biographical
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    primary_number VARCHAR(3),
    birth_date DATE,
    current_age INT,
    birth_city VARCHAR(100),
    birth_state_province VARCHAR(100),
    birth_country VARCHAR(50),

    -- Physical
    height VARCHAR(10),
    weight INT,

    -- Position
    primary_position_code VARCHAR(5),
    primary_position_name VARCHAR(50),

    -- Batting/Throwing
    bat_side_code VARCHAR(1),
    bat_side_description VARCHAR(10),
    pitch_hand_code VARCHAR(1),
    pitch_hand_description VARCHAR(10),

    -- Career
    mlb_debut_date DATE,
    draft_year INT,

    -- Strike zone
    strike_zone_top FLOAT,
    strike_zone_bottom FLOAT,

    -- Game-specific (from boxscore)
    is_starting_lineup BOOLEAN,
    batting_order INT,

    UNIQUE (raw_id, player_id),
    INDEX idx_game_pk (game_pk),
    INDEX idx_player_id (player_id)
);
```

#### Plays (from liveData.plays.allPlays array)
```sql
CREATE TABLE game.live_game_plays (
    id BIGSERIAL PRIMARY KEY,
    raw_id BIGINT REFERENCES game.live_game_v1(id),
    game_pk BIGINT NOT NULL,

    -- Play identification
    at_bat_index INT NOT NULL,
    play_index INT,

    -- Inning context
    inning INT,
    half_inning VARCHAR(10),
    is_top_inning BOOLEAN,

    -- Timing
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    play_end_time TIMESTAMPTZ,

    -- Play result
    result_type VARCHAR(20),  -- atBat, action, etc.
    event VARCHAR(50),        -- Groundout, Single, etc.
    event_type VARCHAR(50),   -- field_out, single, etc.
    description TEXT,

    -- Scoring
    rbi INT,
    away_score INT,
    home_score INT,
    is_scoring_play BOOLEAN,

    -- Count
    balls INT,
    strikes INT,
    outs INT,

    -- Matchup
    batter_id INT,
    batter_name VARCHAR(100),
    bat_side VARCHAR(1),
    pitcher_id INT,
    pitcher_name VARCHAR(100),
    pitch_hand VARCHAR(1),

    -- Flags
    is_complete BOOLEAN,
    has_review BOOLEAN,
    has_out BOOLEAN,
    is_out BOOLEAN,

    -- Captivating index (for highlights)
    captivating_index INT,

    UNIQUE (raw_id, at_bat_index),
    INDEX idx_game_pk (game_pk),
    INDEX idx_inning (inning, half_inning),
    INDEX idx_batter (batter_id),
    INDEX idx_pitcher (pitcher_id)
);
```

#### Runners (from plays[].runners array)
```sql
CREATE TABLE game.live_game_play_runners (
    id BIGSERIAL PRIMARY KEY,
    play_id BIGINT REFERENCES game.live_game_plays(id),
    game_pk BIGINT NOT NULL,
    at_bat_index INT NOT NULL,

    -- Runner identification
    runner_id INT,
    runner_full_name VARCHAR(100),

    -- Movement
    start_base VARCHAR(10),  -- '', '1B', '2B', '3B'
    end_base VARCHAR(10),    -- '1B', '2B', '3B', 'score'

    -- Result
    event VARCHAR(50),
    event_type VARCHAR(50),
    movement_reason VARCHAR(50),

    -- Credits
    responsible_pitcher_id INT,

    -- RBI tracking
    is_out BOOLEAN,
    out_number INT,

    -- Earned run tracking
    is_earned_run BOOLEAN,

    INDEX idx_play_id (play_id),
    INDEX idx_runner_id (runner_id),
    INDEX idx_game_pk (game_pk)
);
```

#### Pitch Events (from plays[].playEvents array)
```sql
CREATE TABLE game.live_game_pitch_events (
    id BIGSERIAL PRIMARY KEY,
    play_id BIGINT REFERENCES game.live_game_plays(id),
    game_pk BIGINT NOT NULL,
    at_bat_index INT NOT NULL,

    -- Event identification
    pitch_index INT NOT NULL,
    event_index INT,

    -- Type
    is_pitch BOOLEAN,
    event_type VARCHAR(20),  -- pitch, action, pickoff, etc.

    -- Timing
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,

    -- Count after this pitch
    balls INT,
    strikes INT,
    outs INT,

    -- Pitch details (if isPitch=true)
    pitch_type_code VARCHAR(5),       -- FF, SL, CH, CU, etc.
    pitch_type_description VARCHAR(50),

    -- Pitch data
    start_speed FLOAT,               -- MPH
    end_speed FLOAT,
    zone INT,                        -- Strike zone (1-14)
    type_confidence FLOAT,

    -- Pitch coordinates
    plate_x FLOAT,
    plate_z FLOAT,
    coordinates_x FLOAT,
    coordinates_y FLOAT,

    -- Pitch break/movement
    break_angle FLOAT,
    break_length FLOAT,
    break_y FLOAT,
    spin_rate INT,                   -- RPM
    spin_direction INT,              -- Degrees

    -- Launch data (batted balls)
    launch_speed FLOAT,              -- Exit velocity (MPH)
    launch_angle FLOAT,              -- Degrees
    total_distance FLOAT,            -- Feet
    trajectory VARCHAR(20),          -- line_drive, fly_ball, ground_ball, popup
    hardness VARCHAR(20),            -- soft, medium, hard
    location INT,                    -- Batted ball zone

    -- Result
    description TEXT,
    call_code VARCHAR(5),            -- B, S, X, etc.
    call_description VARCHAR(50),    -- Ball, Strike, In play, etc.
    is_strike BOOLEAN,
    is_ball BOOLEAN,

    -- Player involved (for actions/pickoffs)
    player_id INT,

    UNIQUE (play_id, pitch_index),
    INDEX idx_game_pk (game_pk),
    INDEX idx_pitch_type (pitch_type_code),
    INDEX idx_launch_speed (launch_speed) WHERE launch_speed IS NOT NULL,
    INDEX idx_is_pitch (is_pitch)
);
```

#### Lineup Changes
```sql
CREATE TABLE game.live_game_lineup_changes (
    id BIGSERIAL PRIMARY KEY,
    raw_id BIGINT REFERENCES game.live_game_v1(id),
    game_pk BIGINT NOT NULL,

    -- When
    inning INT,
    half_inning VARCHAR(10),
    at_bat_index INT,

    -- Who
    team_id INT,
    player_in_id INT,
    player_in_name VARCHAR(100),
    player_out_id INT,
    player_out_name VARCHAR(100),

    -- Position
    position_code VARCHAR(5),
    position_name VARCHAR(50),
    batting_order INT,

    -- Type
    substitution_type VARCHAR(50),  -- Defensive, Pinch Hitter, Pinch Runner, etc.

    INDEX idx_game_pk (game_pk),
    INDEX idx_player_in (player_in_id),
    INDEX idx_player_out (player_out_id)
);
```

#### Scoring Plays (reference table)
```sql
CREATE TABLE game.live_game_scoring_plays (
    id BIGSERIAL PRIMARY KEY,
    raw_id BIGINT REFERENCES game.live_game_v1(id),
    game_pk BIGINT NOT NULL,

    -- Reference to the play
    at_bat_index INT NOT NULL,
    play_id BIGINT REFERENCES game.live_game_plays(id),

    -- Quick reference
    inning INT,
    half_inning VARCHAR(10),
    away_score INT,
    home_score INT,
    runs_scored INT,

    INDEX idx_game_pk (game_pk),
    INDEX idx_play_id (play_id)
);
```

---

## Complete Normalized Table List for game.live_game_v1

1. `game.live_game_metadata` - Top-level game info
2. `game.live_game_players` - All players in the game
3. `game.live_game_plays` - All plays (at-bats and actions)
4. `game.live_game_play_runners` - Runner movement on each play
5. `game.live_game_pitch_events` - Every pitch thrown
6. `game.live_game_lineup_changes` - Substitutions
7. `game.live_game_scoring_plays` - Reference to scoring plays
8. `game.live_game_linescore` - Inning-by-inning scores
9. `game.live_game_linescore_innings` - Individual inning data
10. `game.live_game_boxscore_team_stats` - Team batting/pitching totals
11. `game.live_game_boxscore_batters` - Batter stat lines
12. `game.live_game_boxscore_pitchers` - Pitcher stat lines
13. `game.live_game_officials` - Umpires
14. `game.live_game_decisions` - Winning/losing pitcher, save
15. `game.live_game_mound_visits` - Mound visit tracking
16. `game.live_game_review_challenges` - Replay review tracking
17. `game.live_game_probables` - Probable starting pitchers

**Total: 17 normalized tables from ONE raw table!**

---

## Data Flow Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  pymlb_statsapi: Game.liveGameV1(game_pk=747175)            │
│  Returns: Complete nested JSON (5000+ lines)                 │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│  RAW LAYER: game.live_game_v1                                │
│  - id: 12345                                                  │
│  - game_pk: 747175                                            │
│  - captured_at: 2024-11-09 20:30:00                          │
│  - schema_version: v1                                         │
│  - data: {gamePk: 747175, gameData: {...}, liveData: {...}}  │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│  PySpark ETL Job: flatten_live_game_v1.py                    │
│  - Read from raw table                                        │
│  - Extract JSONPath expressions                               │
│  - Flatten nested arrays                                      │
│  - Join related entities                                      │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│  NORMALIZED LAYER: 17 tables                                 │
│  ├── game.live_game_metadata (1 row)                         │
│  ├── game.live_game_players (51 rows)                        │
│  ├── game.live_game_plays (75 rows)                          │
│  ├── game.live_game_play_runners (120 rows)                  │
│  ├── game.live_game_pitch_events (250 rows)                  │
│  └── ... (12 more tables)                                     │
└──────────────────────────────────────────────────────────────┘
```

---

## Workflow Dependencies

```
Season.seasons()
  ↓
  ├─→ Schedule.schedule(date=X)
  │     ↓
  │     └─→ Game.liveGameV1(game_pk=Y)  [for each game in schedule]
  │           ↓
  │           ├─→ Person.person(personIds=Z)  [for each unique player]
  │           └─→ Team.team(teamId=W)  [for home/away teams]
  │
  └─→ Team.teams(sportId=1)
        ↓
        └─→ Team.roster(teamId=X)
```

---

## Partitioning Strategy

### Time-Series Partitioning
- All raw tables: **Partition by `captured_at` (monthly)**
- Game tables: **Partition by `game_date` (yearly)**
- Player stats: **Partition by `season`**

### Benefits
- Fast queries for recent data
- Easy archival of old data
- Improved index performance
- Simplified data retention policies

---

## Schema Version Management

Track API schema changes:

```sql
CREATE TABLE metadata.schema_versions (
    id SERIAL PRIMARY KEY,
    endpoint VARCHAR(50) NOT NULL,
    method VARCHAR(50) NOT NULL,
    version VARCHAR(10) NOT NULL,
    effective_date DATE NOT NULL,
    schema_definition JSONB NOT NULL,  -- Full JSON schema
    breaking_changes TEXT[],
    added_fields TEXT[],
    deprecated_fields TEXT[],
    notes TEXT,

    UNIQUE (endpoint, method, version)
);
```

Every raw table row has `schema_version` so we can:
1. Handle multiple schema versions simultaneously
2. Perform schema evolution migrations
3. Track API changes over time

---

## Next Steps

1. ✅ Complete schema mapping (this document)
2. Generate SQL DDL from this mapping
3. Create PySpark jobs for each normalized table extraction
4. Build Avro schemas matching these PostgreSQL schemas
5. Implement schema evolution tools
