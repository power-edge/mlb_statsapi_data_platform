# Additional Tables Needed - Discovery Report

**Date**: 2024-11-15
**Based on**: Comprehensive stub data analysis (`game_pk=747175`)

## Executive Summary

After analyzing the complete structure of `game.liveGameV1` stub data, we've identified **8-10 additional tables** that should be added beyond our initial 17 tables.

## Critical Findings

### 1. ⭐ Play Events Should Be Split (HIGH PRIORITY)

**Current**: Single `game.live_game_pitch_events` table
**Problem**: `playEvents` array contains BOTH pitch events AND non-pitch events (substitutions, actions, reviews)

**Recommendation**: Split into 2 tables

#### Table 18: `game.live_game_pitch_events` (already defined)
- Filter: `$.liveData.plays.allPlays[].playEvents[]` WHERE `isPitch = true`
- ~250 pitches per game
- Contains: pitch type, speed, break, coordinates, etc.

#### ✨ NEW TABLE 19: `game.live_game_play_actions`
- Filter: `$.liveData.plays.allPlays[].playEvents[]` WHERE `isPitch = false`
- Contains: substitutions, pickoffs, mound visits, challenges, etc.
- Fields:
  - `game_pk`, `play_id`, `action_index`
  - `action_type` (substitution, pickoff, caught_stealing, etc.)
  - `description`
  - `is_substitution`
  - `player_in_id`, `player_out_id` (for substitutions)
  - `position`
  - `start_time`, `end_time`

**Why**: Mixing pitch data with game actions creates:
- Schema bloat (pitch fields NULL for non-pitch events)
- Query complexity (filtering isPitch everywhere)
- Analytics confusion (counting pitches vs counting actions)

---

### 2. ⭐ Fielding Credits (HIGH PRIORITY)

**Found**: `$.liveData.plays.allPlays[].playEvents[].credits[]`
**Count**: Variable (1-3 credits per defensive play)

#### ✨ NEW TABLE 20: `game.live_game_fielding_credits`
Defensive credits for outs (putouts, assists, errors)

**Fields**:
- `game_pk`, `play_id`, `event_index`, `credit_index`
- `player_id`
- `position`
- `credit_type` (putout, assist, error, etc.)
- `source_captured_at`

**Example**: Ground out to shortstop
- Credit 1: Shortstop (fielded ball) → Assist
- Credit 2: First baseman (caught throw) → Putout

**Why**: Essential for defensive statistics, fielding percentage, etc.

---

### 3. ⭐ Player Box Score Stats (MEDIUM PRIORITY)

**Found**: `$.liveData.boxscore.teams.[home|away].players.ID*.stats`

**Current**: We have `game.live_game_players` but no detailed stats

#### ✨ NEW TABLE 21: `game.live_game_player_batting_stats`
Per-game batting statistics for each batter

**Fields**:
- `game_pk`, `player_id`
- `at_bats`, `runs`, `hits`, `rbi`, `walks`, `strikeouts`
- `doubles`, `triples`, `home_runs`
- `stolen_bases`, `caught_stealing`
- `batting_average`, `obp`, `slg`, `ops`
- `left_on_base`
- `source_captured_at`

#### ✨ NEW TABLE 22: `game.live_game_player_pitching_stats`
Per-game pitching statistics for each pitcher

**Fields**:
- `game_pk`, `player_id`
- `innings_pitched`, `hits_allowed`, `runs_allowed`, `earned_runs`
- `walks`, `strikeouts`, `home_runs_allowed`
- `pitches_thrown`, `strikes`, `balls`
- `era`, `whip`
- `batters_faced`
- `source_captured_at`

#### ✨ NEW TABLE 23: `game.live_game_player_fielding_stats`
Per-game fielding statistics

**Fields**:
- `game_pk`, `player_id`, `position`
- `innings_played`
- `chances`, `putouts`, `assists`, `errors`
- `fielding_percentage`
- `source_captured_at`

**Why**: Critical for player performance analysis, fantasy sports, etc.

---

### 4. Play-by-Inning Summary (LOW PRIORITY)

**Found**: `$.liveData.plays.playsByInning[]`
**Count**: 9 innings

#### ✨ NEW TABLE 24: `game.live_game_inning_summary`
Summary of plays per inning (useful for quick lookups)

**Fields**:
- `game_pk`, `inning`, `half_inning`
- `start_play_index`, `end_play_index`
- `hits_home`, `hits_away`
- `runs_home`, `runs_away`
- `source_captured_at`

**Why**: Quick lookups without scanning all plays

---

### 5. Hits Details (LOW PRIORITY)

**Found**: `$.liveData.plays.playsByInning[].hits.[home|away][]`

#### ✨ NEW TABLE 25: `game.live_game_hits`
Details about each hit (separate from plays)

**Fields**:
- `game_pk`, `inning`, `hit_index`
- `team` (home/away)
- `batter_id`, `pitcher_id`
- `hit_type` (single, double, triple, home run)
- `coordinates_x`, `coordinates_y`
- `description`
- `source_captured_at`

**Why**: Spray charts, hit analysis, etc.

---

### 6. Top Performers (LOW PRIORITY)

**Found**: `$.liveData.boxscore.topPerformers[]`

#### ✨ NEW TABLE 26: `game.live_game_top_performers`
MLB-designated top performers for the game

**Fields**:
- `game_pk`, `performer_index`
- `player_id`
- `performance_type` (batting, pitching, fielding)
- `game_score` (for batters)
- `pitching_game_score` (for pitchers)
- `source_captured_at`

---

### 7. Player Positions (for multi-position players) (LOW PRIORITY)

**Found**: `$.liveData.boxscore.teams.[home|away].players.ID*.allPositions[]`

#### ✨ NEW TABLE 27: `game.live_game_player_positions`
All positions played by each player in the game

**Fields**:
- `game_pk`, `player_id`, `position_index`
- `position_code` (P, C, 1B, 2B, etc.)
- `position_name`
- `position_abbreviation`
- `source_captured_at`

**Why**: Some players play multiple positions in a game

---

## Recommended Table Additions Summary

| Priority | Table Name | Why |  Rows per Game |
|----------|------------|-----|----------------|
| 🔥 **High** | `game.live_game_play_actions` | Split non-pitch events from pitches | ~10-20 |
| 🔥 **High** | `game.live_game_fielding_credits` | Defensive credits (putouts, assists) | ~50-75 |
| 🟡 **Medium** | `game.live_game_player_batting_stats` | Per-player batting stats | ~18 |
| 🟡 **Medium** | `game.live_game_player_pitching_stats` | Per-player pitching stats | ~6-10 |
| 🟡 **Medium** | `game.live_game_player_fielding_stats` | Per-player fielding stats | ~18 |
| 🟢 **Low** | `game.live_game_inning_summary` | Inning-level summaries | 9-18 |
| 🟢 **Low** | `game.live_game_hits` | Hit details with coordinates | ~15-20 |
| 🟢 **Low** | `game.live_game_top_performers` | MLB top performers | 2-3 |
| 🟢 **Low** | `game.live_game_player_positions` | Multi-position tracking | ~0-5 |

**Total**: 17 (current) + 9 (recommended) = **26 tables**

---

## Implementation Recommendations

### Phase 1: Critical Splits (Do Now)
1. Split `playEvents` into `pitch_events` and `play_actions`
2. Add `fielding_credits` table

### Phase 2: Player Stats (Next Week)
3. Add batting/pitching/fielding stats tables

### Phase 3: Analytics Enhancement (Later)
4. Add remaining tables as needed for specific analytics

---

## Questions to Resolve

1. **Runner Credits**: Should we also split `$.liveData.plays.allPlays[].runners[].credits[]` into a separate table?
   - Currently bundled with runners
   - Could be separate `game.live_game_runner_credits` table

2. **Review Details**: Do we need a `game.live_game_reviews` table for replay challenges?
   - Path: `$.liveData.plays.allPlays[].reviewDetails`
   - Not common (maybe 1-2 per game, if any)

3. **Game Info/Notes**: Should we extract `$.liveData.boxscore.info[]` and `teams.[home|away].info[]`?
   - Miscellaneous game information (weather, attendance, etc.)
   - Could be separate `game.live_game_info` table

---

## Next Steps

1. **Review & Approve**: Decide which tables to add now vs later
2. **Update Schema Mapping**: Add definitions for approved tables
3. **Generate DDL**: Create migration SQL for all tables
4. **Test with Stub Data**: Validate extractions work correctly

---

**Recommendation**: Start with **Phase 1** (critical splits) immediately. This ensures clean separation of concerns and better query performance.
