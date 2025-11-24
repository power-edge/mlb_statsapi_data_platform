# features/schedule_ingestion.feature
# BDD tests for MLB schedule data ingestion pipeline
#
# Tags:
#   @integration - Requires database connection
#   @local-only - Should NEVER run in production/CI (truncates data)
#   @smoke - Critical path tests

@integration @local-only
Feature: Schedule Data Ingestion
  As a data engineer
  I want to ingest MLB schedule data from the Stats API
  So that I can track games and build game ingestion pipelines

  Background:
    Given a clean test database
    And a configured MLB Stats API client for schedule data
    And the client is using stub mode for deterministic testing

  @smoke
  Scenario: Successfully ingest schedule for a specific date
    When I ingest schedule data for date "2024-07-04" and sport ID 1
    Then the schedule data should be saved to PostgreSQL
    And the raw table should contain 1 schedule record
    And the schedule record should have schema version "v1"
    And the schedule record should have response status 200

  @smoke
  Scenario: Extract schedule_date from request parameters (partition key)
    When I ingest schedule data for date "2024-07-04" and sport ID 1
    Then the schedule_date field should be extracted as "2024-07-04"
    And the schedule_date should be a date type in PostgreSQL
    And the request parameters should contain date="2024-07-04"

  @smoke
  Scenario: Validate schedule data structure with games
    When I ingest schedule data for date "2024-07-04" and sport ID 1
    Then the schedule data should contain required fields:
      | field       |
      | totalGames  |
      | dates       |
    And the totalGames should be greater than or equal to 0

  Scenario: Schedule data contains game list for active date
    When I ingest schedule data for date "2024-07-04" and sport ID 1
    Then the JSONB data should contain a "dates" array
    And if totalGames is greater than 0, the dates array should have games
    And each game should have required fields:
      | field    |
      | gamePk   |
      | gameType |
      | season   |
      | gameDate |

  Scenario: Extract sport_id from request parameters
    When I ingest schedule data for date "2024-07-04" and sport ID 1
    Then the sport_id field should be extracted as 1
    And the request parameters should be stored as JSONB
    And the request parameters should contain sportId=1

  Scenario: Capture request and response metadata
    When I ingest schedule data for date "2024-07-04" and sport ID 1
    Then the schedule record should have a valid source URL
    And the source URL should contain "schedule"
    And the captured_at timestamp should be present
    And the ingestion_timestamp should be present

  Scenario: Fetch schedule data without saving (dry run)
    When I fetch schedule data for date "2024-07-04" without saving
    Then the API response should contain schedule data
    And the response should have metadata
    And no database record should be created

  Scenario: Idempotent ingestion creates multiple records
    When I ingest schedule data for date "2024-07-04" and sport ID 1
    And I ingest the same schedule data again
    Then the raw table should contain 2 schedule records
    And each record should have a unique ID
    And each record should have a different ingestion timestamp

  @smoke
  Scenario: Schedule data for date with no games
    When I ingest schedule data for a date with no games
    Then the schedule data should be saved to PostgreSQL
    And the totalGames should be 0
    And the dates array may be empty
    And the ingestion should still succeed

  Scenario: Validate date format in schedule data
    When I ingest schedule data for date "2024-07-04" and sport ID 1
    And the schedule contains games
    Then each game date should be in YYYY-MM-DD format
    And the date in the dates array should match "2024-07-04"

  Scenario: Handle missing schema metadata gracefully
    Given the schedule schema metadata is registered
    Then the schema should have endpoint "schedule"
    And the schema should have method "schedule"
    And the schema should have field definitions
    And the schedule_date field should be marked as partition key

  @regression
  Scenario: Verify schedule metadata consistency
    When I ingest schedule data for date "2024-07-04" and sport ID 1
    Then the schedule record should have:
      | field            | value         |
      | schema_version   | v1            |
      | response_status  | 200           |
      | schedule_date    | 2024-07-04    |
      | sport_id         | 1             |
    And the data column should be valid JSONB
    And the source_url should not be empty

  @regression
  Scenario: Partition created based on schedule_date
    When I ingest schedule data for date "2024-07-04" and sport ID 1
    Then the data should be stored in the schedule_2024 partition
    And the partition key should match the extracted schedule_date

  @regression
  Scenario Outline: Ingest schedules for multiple dates
    When I ingest schedule data for date "<date>" and sport ID 1
    Then the schedule_date field should be extracted as "<date>"
    And the data should be stored in the partition for year <year>

    Examples:
      | date       | year |
      | 2024-07-04 | 2024 |
      | 2024-09-15 | 2024 |
      | 2025-03-20 | 2025 |
