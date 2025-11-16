# features/raw_ingestion.feature
# BDD tests for raw data ingestion layer
#
# Tags:
#   @integration - Requires database connection
#   @local-only - Should NEVER run in production/CI (truncates data)
#   @smoke - Critical path tests
#   @regression - Full regression suite

Feature: Raw Data Ingestion Layer
  As a data engineer
  I want to ingest MLB Stats API responses into PostgreSQL
  So that I can maintain complete historical data with full replay capability

  Background:
    Given a clean test database
    And the RawStorageClient is initialized

  @integration @local-only @smoke
  Scenario: Ingest single live game with all metadata
    Given I have a valid live game API response for game_pk 747175
    When I save the live game to the raw table
    Then the raw table should contain 1 record
    And the record should have game_pk 747175
    And the record should have captured_at timestamp
    And the record should have endpoint "game"
    And the record should have method "liveGameV1"
    And the record should have status_code 200
    And the data column should be valid JSONB
    And the data should contain key "gamePk"
    And the data should contain key "gameData"
    And the data should contain key "liveData"

  @integration @local-only @regression
  Scenario: Ingest multiple versions of same game (append-only)
    Given I have a valid live game API response for game_pk 747175
    When I save the live game at "2024-11-15T20:00:00Z"
    And I save the same game at "2024-11-15T20:30:00Z"
    And I save the same game at "2024-11-15T21:00:00Z"
    Then the raw table should contain 3 records for game_pk 747175
    And each record should have a different captured_at timestamp
    And the records should be ordered by captured_at ascending
    And the composite primary key (game_pk, captured_at) should be unique

  @integration @local-only @regression
  Scenario Outline: Ingest responses with different status codes
    Given I have an API response for game_pk <game_pk> with status_code <status_code>
    When I save the live game to the raw table
    Then the raw table should contain a record with game_pk <game_pk>
    And the record should have status_code <status_code>
    And the ingestion should <result>

    Examples: Successful Responses
      | game_pk | status_code | result  |
      | 747175  | 200         | succeed |
      | 747176  | 201         | succeed |

    Examples: Client Errors
      | game_pk | status_code | result  |
      | 747177  | 404         | succeed |
      | 747178  | 429         | succeed |

    Examples: Server Errors
      | game_pk | status_code | result  |
      | 747179  | 500         | succeed |
      | 747180  | 503         | succeed |

  @integration @local-only @smoke
  Scenario: Query latest version of game
    Given I have ingested 5 versions of game_pk 747175
    When I query for the latest version of game_pk 747175
    Then I should receive the version with the most recent captured_at
    And the version should contain the latest game state

  @integration @local-only @regression
  Scenario: Query game history (all versions)
    Given I have ingested the following versions of game_pk 747175:
      | captured_at         | abstract_game_state |
      | 2024-11-15T19:00:00Z | Preview             |
      | 2024-11-15T20:00:00Z | Live                |
      | 2024-11-15T21:00:00Z | Live                |
      | 2024-11-15T22:00:00Z | Final               |
    When I query the history for game_pk 747175
    Then I should receive 4 versions
    And the versions should show game state progression from Preview to Final

  @integration @local-only @regression
  Scenario: Query games by date range
    Given I have ingested games with the following dates:
      | game_pk | captured_at         |
      | 747175  | 2024-11-14T20:00:00Z |
      | 747176  | 2024-11-15T20:00:00Z |
      | 747177  | 2024-11-16T20:00:00Z |
    When I query for games captured between "2024-11-15" and "2024-11-17"
    Then I should receive 2 games
    And the games should be 747176 and 747177

  @integration @local-only @regression
  Scenario: JSONB querying with PostgreSQL operators
    Given I have ingested game_pk 747175 with the following data:
      | home_team           | away_team           | home_score | away_score |
      | Arizona Diamondbacks | Toronto Blue Jays   | 5          | 4          |
    When I query using JSONB operators:
      """
      data->'gameData'->'teams'->'home'->>'name'
      """
    Then I should receive "Arizona Diamondbacks"

  @integration @local-only @smoke
  Scenario: Handle duplicate ingestion attempts (same composite key)
    Given I have a valid live game API response for game_pk 747175
    And I save the live game at "2024-11-15T20:00:00Z"
    When I attempt to save the same game at "2024-11-15T20:00:00Z" again
    Then the ingestion should fail
    And the raw table should contain 1 record

  @integration @local-only @regression
  Scenario: Verify data integrity after ingestion
    Given I have ingested 100 games
    When I query the raw table
    Then all records should have non-null game_pk
    And all records should have non-null captured_at
    And all records should have non-null data
    And all data columns should be valid JSONB
    And all data should contain the gamePk field
    And the gamePk in data should match the game_pk column

  @integration @local-only @regression
  Scenario: Ingest game with missing optional fields
    Given I have an API response for game_pk 747175 with missing optional fields:
      | field          |
      | weather        |
      | venue.location |
    When I save the live game to the raw table
    Then the ingestion should succeed
    And the data should contain null values for missing fields
    And the record should be queryable

  @integration @local-only @smoke
  Scenario: Verify storage size efficiency
    Given I have a valid live game API response of approximately 850 KB
    When I save the live game to the raw table
    Then the JSONB column should store the data efficiently
    And the storage size should be comparable to the original JSON size
    And JSONB indexing should be available for top-level fields

  @integration @local-only @regression
  Scenario: Concurrent ingestion of different games
    Given I have API responses for the following games:
      | game_pk |
      | 747175  |
      | 747176  |
      | 747177  |
      | 747178  |
      | 747179  |
    When I ingest all games concurrently
    Then all 5 games should be successfully stored
    And there should be no data corruption
    And each game should be independently queryable

  @integration @local-only @regression
  Scenario: Verify captured_at preserves timezone information
    Given I have a live game response captured at "2024-11-15T20:30:45.123456+00:00"
    When I save the live game to the raw table
    Then the captured_at should preserve the timezone as UTC
    And timezone conversions should work correctly
    And timestamp comparisons should be accurate

  @integration @local-only @regression
  Scenario: Query unprocessed games for incremental processing
    Given I have ingested the following games:
      | game_pk | captured_at         | processed |
      | 747175  | 2024-11-15T20:00:00Z | true      |
      | 747176  | 2024-11-15T20:30:00Z | false     |
      | 747177  | 2024-11-15T21:00:00Z | false     |
    When I query for unprocessed games since "2024-11-15T20:00:00Z"
    Then I should receive 2 games
    And the games should be 747176 and 747177
    And they should be ordered by captured_at ascending

  @integration @local-only @smoke
  Scenario: Rollback on ingestion failure
    Given I have started a transaction
    And I have ingested 3 games successfully
    When an error occurs during the 4th game ingestion
    Then the transaction should rollback
    And the raw table should contain 0 records from this transaction
    And the database should remain in a consistent state

  @integration @local-only @regression
  Scenario: Metadata consistency check
    Given I have ingested game_pk 747175 with metadata:
      | endpoint | method      | params         | url                                              | status_code |
      | game     | liveGameV1  | {"game_pk": 747175} | https://statsapi.mlb.com/api/v1.1/game/747175/feed/live | 200         |
    When I query the raw record
    Then the endpoint should match the ingested value
    And the method should match the ingested value
    And the params should be valid JSONB
    And the url should match the ingested value
    And the status_code should match the ingested value
