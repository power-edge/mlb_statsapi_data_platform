# features/pipeline_orchestration.feature
# BDD tests for MLB pipeline orchestration
#
# Tags:
#   @integration - Requires database connection
#   @local-only - Should NEVER run in production/CI (truncates data)
#   @smoke - Critical path tests
#   @pipeline - Pipeline orchestration tests

@integration @local-only @pipeline
Feature: Pipeline Orchestration
  As a data engineer
  I want to orchestrate the full data ingestion pipeline
  So that I can ingest Season → Schedule → Game data hierarchically

  Background:
    Given a clean test database
    And a configured pipeline orchestrator
    And the orchestrator is using stub mode for deterministic testing

  # =========================================================================
  # DAILY PIPELINE SCENARIOS
  # =========================================================================

  @smoke
  Scenario: Run daily pipeline for a specific date
    When I run the daily pipeline for date "2024-07-04"
    Then the pipeline should complete successfully
    And the schedule data should be saved to PostgreSQL
    And games should be fetched for that date
    And the pipeline result should show games fetched

  @smoke
  Scenario: Daily pipeline fetches schedule and games
    When I run the daily pipeline for date "2024-07-04"
    Then the schedule table should have at least 1 record
    And the game table should have at least 1 record
    And each game should have a valid game_pk

  @smoke
  Scenario: Pipeline tracks statistics correctly
    When I run the daily pipeline for date "2024-07-04"
    Then the pipeline result should contain:
      | metric             | expected |
      | schedules_fetched  | 1        |
      | games_fetched      | > 0      |
      | errors             | 0        |

  # =========================================================================
  # SCHEDULE EXTRACTION SCENARIOS
  # =========================================================================

  @smoke
  Scenario: Extract game list from schedule
    When I fetch the schedule for date "2024-07-04"
    Then the schedule should contain multiple games
    And each game should have:
      | field              |
      | game_pk            |
      | game_date          |
      | abstract_game_state|
      | home_team_id       |
      | away_team_id       |

  Scenario: Filter games by status
    Given I have fetched the schedule for date "2024-07-04"
    When I filter for final games
    Then all returned games should have status "Final"

  Scenario: Identify active games for polling
    Given I have fetched the schedule for date "2024-07-04"
    When I get active games
    Then returned games should be either "Live" or "Preview"

  # =========================================================================
  # GAME DATA SCENARIOS
  # =========================================================================

  @smoke
  Scenario: Fetch single game data
    When I fetch game data for game_pk 746385
    Then the game data should be saved to PostgreSQL
    And the game record should contain gameData
    And the game record should contain liveData

  Scenario: Fetch multiple games efficiently
    When I fetch games for game_pks: 746385, 746386, 746387
    Then all 3 games should be saved
    And each game should have a unique captured_at timestamp

  Scenario: Game data contains team information
    When I fetch game data for game_pk 746385
    Then the game data should contain home team info
    And the game data should contain away team info
    And team IDs should be extractable

  # =========================================================================
  # SEASON DATA SCENARIOS
  # =========================================================================

  Scenario: Fetch season date ranges
    When I fetch season data
    Then I should receive season date ranges
    And each season should have:
      | field                  |
      | season_id              |
      | regular_season_start   |
      | regular_season_end     |

  Scenario: Get current season
    When I get the current season
    Then I should receive a SeasonDates object
    And the season should have a valid date range

  # =========================================================================
  # BACKFILL SCENARIOS
  # =========================================================================

  @smoke
  Scenario: Backfill a date range
    When I backfill from "2024-07-04" to "2024-07-05"
    Then the pipeline should process 2 days
    And schedules should be fetched for each day
    And games should be fetched for each day

  Scenario: Backfill with season boundaries
    When I backfill season "2024" with Spring Training excluded
    Then the backfill should start from regular season start
    And the backfill should end at postseason end or regular season end

  # =========================================================================
  # STORAGE ADAPTER SCENARIOS
  # =========================================================================

  @smoke
  Scenario: Storage adapter maps endpoints to tables correctly
    Given a storage adapter connected to PostgreSQL
    When I store season data via the adapter
    Then the data should be in table "season.seasons"
    And the adapter should track insert count

  Scenario: Storage adapter handles game timestamps mapping
    Given a storage adapter connected to PostgreSQL
    When I store game timestamp data via the adapter
    Then the data should be in table "game.live_game_timestamps"
    And the special table mapping should be applied

  Scenario: Storage adapter tracks statistics
    Given a storage adapter connected to PostgreSQL
    When I store multiple records via the adapter
    Then the adapter stats should show:
      | stat      | expected |
      | inserts   | > 0      |
      | errors    | 0        |

  # =========================================================================
  # ERROR HANDLING SCENARIOS
  # =========================================================================

  Scenario: Handle API errors gracefully
    Given an API that returns errors for some games
    When I run the daily pipeline
    Then successful games should still be saved
    And errors should be tracked in the result
    And the pipeline should not crash

  Scenario: Handle database connection issues
    Given the database is temporarily unavailable
    When I attempt to run the pipeline
    Then an appropriate error should be raised
    And no partial data should be committed

  # =========================================================================
  # ENRICHMENT SCENARIOS
  # =========================================================================

  Scenario: Extract roster from game data
    When I fetch game data for game_pk 746385
    Then I should be able to extract the roster
    And the roster should contain player IDs
    And the roster should contain home and away team IDs

  Scenario: Pipeline with enrichment disabled
    Given enrichment is disabled in the config
    When I run the daily pipeline for date "2024-07-04"
    Then player data should not be fetched
    And team data should not be fetched
    And the pipeline should complete faster

  # =========================================================================
  # LIVE GAME SCENARIOS
  # =========================================================================

  Scenario: Fetch game timestamps for completed game
    When I fetch timestamps for game_pk 746385
    Then I should receive a list of timestamp strings
    And each timestamp should be in YYYYMMDD_HHMMSS format

  Scenario: Live poller configuration
    Given a live game poller with 30 second interval
    Then the poll interval should be 30 seconds
    And the poller should use the orchestrator's API client

  # =========================================================================
  # DATA QUALITY SCENARIOS
  # =========================================================================

  @regression
  Scenario: Verify game type coverage
    When I backfill date "2024-03-15"
    Then games should include Spring Training type "S"

  @regression
  Scenario: Verify regular season games
    When I backfill date "2024-07-04"
    Then games should include Regular Season type "R"

  @regression
  Scenario: Verify postseason games
    When I backfill date "2024-10-30"
    Then games should include World Series type "W"

  # =========================================================================
  # IDEMPOTENCY SCENARIOS
  # =========================================================================

  Scenario: Pipeline is idempotent
    When I run the daily pipeline for date "2024-07-04"
    And I run the daily pipeline for date "2024-07-04" again
    Then both runs should succeed
    And the database should have records from both runs
    And no duplicate key errors should occur
