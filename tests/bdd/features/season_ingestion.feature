# features/season_ingestion.feature
# BDD tests for MLB season data ingestion pipeline
#
# Tags:
#   @integration - Requires database connection
#   @local-only - Should NEVER run in production/CI (truncates data)
#   @smoke - Critical path tests

@integration @local-only
Feature: Season Data Ingestion
  As a data engineer
  I want to ingest MLB season data from the Stats API
  So that I can track season schedules and metadata

  Background:
    Given a clean test database
    And a configured MLB Stats API client for season data
    And the client is using stub mode for deterministic testing

  @smoke
  Scenario: Successfully ingest current MLB season
    When I ingest season data for sport ID 1
    Then the season data should be saved to PostgreSQL
    And the raw table should contain 1 season record
    And the season record should have schema version "v1"
    And the season record should have response status 200

  @smoke
  Scenario: Validate season data structure
    When I ingest season data for sport ID 1
    Then the season data should contain required fields:
      | field                    |
      | seasonId                 |
      | regularSeasonStartDate   |
      | regularSeasonEndDate     |
      | seasonStartDate          |
      | seasonEndDate            |
    And all date fields should be in YYYY-MM-DD format

  @smoke
  Scenario: Extract sport_id from request parameters
    When I ingest season data for sport ID 1
    Then the sport_id field should be extracted as 1
    And the request parameters should be stored as JSONB
    And the request parameters should contain sportId=1

  Scenario: Capture request and response metadata
    When I ingest season data for sport ID 1
    Then the season record should have a valid source URL
    And the source URL should contain "season"
    And the captured_at timestamp should be present
    And the ingestion_timestamp should be present

  Scenario: Fetch season data without saving (dry run)
    When I fetch season data without saving
    Then the API response should contain season data
    And the response should have metadata
    And no database record should be created

  Scenario: Idempotent ingestion creates multiple records
    When I ingest season data for sport ID 1
    And I ingest the same season data again
    Then the raw table should contain 2 season records
    And each record should have a unique ID
    And each record should have a different ingestion timestamp

  Scenario: Handle missing schema metadata gracefully
    Given the season schema metadata is registered
    Then the schema should have endpoint "season"
    And the schema should have method "seasons"
    And the schema should have field definitions

  Scenario: Season data contains multiple seasons
    When I ingest season data for sport ID 1
    Then the JSONB data should contain a "seasons" array
    And the seasons array should have at least 1 season
    And each season should have a seasonId

  @regression
  Scenario: Verify season metadata consistency
    When I ingest season data for sport ID 1
    Then the season record should have:
      | field            | value   |
      | schema_version   | v1      |
      | response_status  | 200     |
    And the data column should be valid JSONB
    And the source_url should not be empty

  @regression
  Scenario: Concurrent season ingestion
    When I ingest season data 3 times concurrently
    Then the raw table should contain 3 season records
    And all records should have unique IDs
    And there should be no data corruption
