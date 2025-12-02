# features/game_ingestion.feature
# BDD tests for MLB game data ingestion pipeline
#
# Tags:
#   @integration - Requires database connection
#   @local-only - Should NEVER run in production/CI (truncates data)
#   @smoke - Critical path tests

@integration @local-only
Feature: Game Data Ingestion
  As a data engineer
  I want to ingest MLB live game data from the Stats API
  So that I can track play-by-play data and build analytics

  Background:
    Given a clean test database
    And a configured MLB Stats API client for game data
    And the client is using stub mode for deterministic testing

  @smoke
  Scenario: Successfully ingest live game data
    When I ingest live game data for game_pk 744834
    Then the game data should be saved to PostgreSQL
    And the raw table should contain 1 game record
    And the game record should have schema version "v1"
    And the game record should have response status 200

  @smoke
  Scenario: Extract all game fields from nested JSON
    When I ingest live game data for game_pk 744834
    Then the following fields should be extracted correctly:
      | field         | value                 |
      | game_pk       | 744834                |
      | game_date     | 2024-07-04            |
      | season        | 2024                  |
      | game_type     | R                     |
      | home_team_id  | 120                   |
      | away_team_id  | 121                   |
      | venue_id      | 3309                  |
    And the game_state should be one of "Final", "Live", or "Preview"

  @smoke
  Scenario: Validate game data complex structure
    When I ingest live game data for game_pk 744834
    Then the JSONB data should contain required top-level fields:
      | field     |
      | gamePk    |
      | gameData  |
      | liveData  |
    And the gameData should contain:
      | field    |
      | datetime |
      | teams    |
      | players  |
      | venue    |
    And the liveData should contain plays or linescore data

  Scenario: Game data contains complete team information
    When I ingest live game data for game_pk 744834
    Then the gameData teams should have home and away teams
    And each team should have an ID
    And the home team ID should not equal the away team ID

  Scenario: Game datetime fields are valid
    When I ingest live game data for game_pk 744834
    Then the gameData datetime should have an officialDate field
    And the officialDate should be in YYYY-MM-DD format
    And the officialDate should match the extracted game_date

  Scenario: Capture request and response metadata
    When I ingest live game data for game_pk 744834
    Then the game record should have a valid source URL
    And the source URL should contain "game"
    And the request parameters should contain game_pk=744834
    And the captured_at timestamp should be present
    And the ingestion_timestamp should be present

  Scenario: Fetch game data without saving (dry run)
    When I fetch game data for game_pk 744834 without saving
    Then the API response should contain game data
    And the response should have metadata
    And no database record should be created

  Scenario: Idempotent ingestion creates multiple records
    When I ingest live game data for game_pk 744834
    And I ingest the same game data again
    Then the raw table should contain 2 game records
    And each record should have a unique ID
    And each record should have a different ingestion timestamp
    And both records should have the same game_pk

  Scenario: Extract game_pk from response data (not request params)
    When I ingest live game data for game_pk 744834
    Then the game_pk field should be extracted from the response JSON
    And the game_pk should equal 744834
    And the gamePk in JSONB data should match the game_pk column

  Scenario: Handle missing schema metadata gracefully
    Given the game schema metadata is registered
    Then the schema should have endpoint "game"
    And the schema should have method "liveGameV1"
    And the schema should have field definitions
    And the game_date field should be marked as partition key

  @regression
  Scenario: Verify game metadata consistency
    When I ingest live game data for game_pk 744834
    Then the game record should have:
      | field            | value      |
      | schema_version   | v1         |
      | response_status  | 200        |
      | game_pk          | 744834     |
      | game_date        | 2024-07-04 |
    And the data column should be valid JSONB
    And the source_url should not be empty

  @regression
  Scenario: Partition created based on game_date
    When I ingest live game data for game_pk 744834
    Then the data should be stored in the live_game_v1_2024 partition
    And the partition key should match the extracted game_date year

  @regression
  Scenario: Game data size is substantial
    When I ingest live game data for game_pk 744834
    Then the JSONB data should be larger than 100 KB
    And the data should contain extensive play-by-play information
    And PostgreSQL should store the JSONB efficiently

  @smoke
  Scenario Outline: Ingest games from different seasons
    When I ingest live game data for game_pk <game_pk>
    Then the season field should be extracted as "<season>"
    And the data should be stored in the partition for year <year>

    Examples:
      | game_pk | season | year |
      | 744834  | 2024   | 2024 |
      | 715838  | 2023   | 2023 |

  @regression
  Scenario: Game data contains player information
    When I ingest live game data for game_pk 744834
    Then the gameData should have a players object
    And the players object should contain multiple player IDs
    And each player should have basic information

  @regression
  Scenario: Game data contains venue information
    When I ingest live game data for game_pk 744834
    Then the gameData should have a venue object
    And the venue should have an ID of 3309
    And the venue should have a name

  Scenario: Live data contains game state information
    When I ingest live game data for game_pk 744834
    Then the gameData status should indicate the game state
    And the game state should be extracted to the game_state column
    And the game state should be a valid state

  @regression
  Scenario: Track game progression through multiple ingestions
    When I ingest live game data for game_pk 744834 at timestamp "2024-07-04T20:00:00Z"
    And I ingest the same game at timestamp "2024-07-04T21:00:00Z"
    And I ingest the same game at timestamp "2024-07-04T22:00:00Z"
    Then the raw table should contain 3 records for game_pk 744834
    And each record should have a different captured_at timestamp
    And the records should show game progression
