# features/transformation.feature
# BDD tests for transformation layer (raw JSONB → normalized tables)
#
# Tags:
#   @integration - Requires database connection
#   @local-only - Should NEVER run in production/CI (truncates data)
#   @smoke - Critical path tests
#   @transformation - Transformation-specific tests

Feature: Data Transformation Layer
  As a data engineer
  I want to transform raw JSONB data into normalized relational tables
  So that I can enable fast analytics queries while preserving complete historical data

  Background:
    Given a clean test environment
    And I have raw game data ingested for game_pk 747175

  @integration @local-only @smoke @transformation
  Scenario: Transform raw JSONB to normalized metadata table
    When I run the metadata transformation
    Then the normalized metadata table should contain 1 record
    And the record should have game_pk 747175
    And the record should have extracted field "home_team_name"
    And the record should have extracted field "away_team_name"
    And the record should have extracted field "home_score"
    And the record should have extracted field "away_score"
    And the record should have extracted field "venue_name"
    And the record should have extracted field "weather_condition"
    And the record should have a transform_timestamp

  @integration @local-only @regression @transformation
  Scenario: Defensive upsert handles duplicate transformations
    Given I have already transformed game_pk 747175
    When I run the metadata transformation again
    Then the normalized metadata table should still contain 1 record
    And the record should have an updated transform_timestamp
    And the data should match the latest raw version

  @integration @local-only @smoke @transformation
  Scenario: Extract 27 fields from JSONB correctly
    When I run the metadata transformation
    Then the normalized metadata should contain exactly 27 extracted fields
    And all required fields should be non-null:
      | field_name        |
      | game_pk           |
      | game_date         |
      | home_team_id      |
      | away_team_id      |
      | home_team_name    |
      | away_team_name    |
    And optional fields may be null:
      | field_name        |
      | weather_condition |
      | weather_temp      |
      | weather_wind      |

  @integration @local-only @regression @transformation
  Scenario: Transform preserves data lineage
    When I run the metadata transformation
    Then the normalized record should have source_captured_at matching the raw record
    And the normalized record should have source_raw_id
    And the normalized record should have transform_timestamp > source_captured_at

  @integration @local-only @regression @transformation
  Scenario: Handle missing optional fields gracefully
    Given I have raw game data with missing weather information
    When I run the metadata transformation
    Then the transformation should succeed
    And the weather_condition field should be null
    And the weather_temp field should be null
    And other required fields should be populated

  @integration @local-only @smoke @transformation
  Scenario: Transform multiple games in batch
    Given I have raw game data ingested for the following games:
      | game_pk |
      | 747175  |
      | 747176  |
      | 747177  |
    When I run the metadata transformation for all games
    Then the normalized metadata table should contain 3 records
    And each game should have its own normalized record
    And all records should have unique game_pk values

  @integration @local-only @regression @transformation
  Scenario: Transformation is idempotent
    When I run the metadata transformation
    And I run the metadata transformation again
    And I run the metadata transformation again
    Then the normalized metadata table should contain 1 record
    And the result should be identical to running once

  @integration @local-only @regression @transformation
  Scenario: Handle NULL values in JSONB
    Given I have raw game data with null values in optional fields
    When I run the metadata transformation
    Then the transformation should succeed
    And null JSONB values should map to null SQL columns
    And the record should be queryable

  @integration @local-only @smoke @transformation
  Scenario: Verify normalization improves query performance
    Given I have raw game data for game_pk 747175
    When I query the raw JSONB table for home_team_name
    And I query the normalized table for home_team_name
    Then both queries should return "Arizona Diamondbacks"
    And the normalized query should be faster

  @integration @local-only @regression @transformation
  Scenario: Transform only new raw data (incremental processing)
    Given I have transformed games up to captured_at "2024-11-15T20:00:00Z"
    And I have new raw data captured at "2024-11-15T21:00:00Z"
    When I run incremental transformation
    Then only the new raw data should be transformed
    And the previously transformed data should remain unchanged

  @integration @local-only @regression @transformation
  Scenario: Transformation failure doesn't corrupt existing data
    Given I have already transformed game_pk 747175 successfully
    When transformation fails for a new raw record
    Then the existing normalized record should remain intact
    And the database should be in a consistent state
    And I can retry the failed transformation

  @integration @local-only @smoke @transformation
  Scenario: End-to-end pipeline validation
    Given I have no data in raw or normalized tables
    When I ingest a game from the API
    And I run the transformation
    And I query the normalized table
    Then I should get the complete game summary:
      | field             | value                 |
      | home_team_name    | Arizona Diamondbacks  |
      | away_team_name    | Toronto Blue Jays     |
      | home_score        | 5                     |
      | away_score        | 4                     |
      | abstract_game_state | Final               |
      | venue_name        | Chase Field           |

  @integration @local-only @regression @transformation
  Scenario: Verify data types are correct after transformation
    When I run the metadata transformation
    Then the game_pk should be INTEGER type
    And the game_date should be DATE type
    And the game_datetime should be TIMESTAMPTZ type
    And the home_score should be INTEGER type
    And the weather_temp should be INTEGER type
    And the home_team_name should be VARCHAR type

  @integration @local-only @regression @transformation
  Scenario: Handle game state progression through versions
    Given I have the following raw versions for game_pk 747175:
      | captured_at         | abstract_game_state |
      | 2024-11-15T19:00:00Z | Preview             |
      | 2024-11-15T21:00:00Z | Live                |
      | 2024-11-15T23:00:00Z | Final               |
    When I transform each version
    Then the latest normalized record should show "Final"
    And the transform_timestamp should reflect the latest transformation

  @integration @local-only @smoke @transformation
  Scenario: Verify transformation completeness (no data loss)
    Given I have raw game data with complete API response
    When I run the metadata transformation
    Then all 27 metadata fields should be extracted
    And no data from the raw JSONB should be lost
    And I can recreate the normalized record from raw at any time
