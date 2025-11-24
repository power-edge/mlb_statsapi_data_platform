#!/bin/bash
# Test transformation jobs using Docker Compose
# Usage: ./scripts/test_transform.sh [season|schedule|all]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
print_header() {
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

# Check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    print_success "Docker is running"
}

# Build Spark image
build_spark_image() {
    print_header "Building Spark Docker Image"

    if docker compose build spark; then
        print_success "Spark image built successfully"
    else
        print_error "Failed to build Spark image"
        exit 1
    fi
}

# Check if PostgreSQL has data
check_postgres_data() {
    local table=$1
    local table_name=$2

    print_info "Checking if $table_name has data..."

    local count=$(docker compose exec -T postgres psql -U mlb_admin -d mlb_games -t -A -c "SELECT COUNT(*) FROM $table;")

    if [ "$count" -eq "0" ]; then
        print_warning "No data in $table_name (count: $count)"
        return 1
    else
        print_success "Found $count records in $table_name"
        return 0
    fi
}

# Insert test season data
insert_test_season_data() {
    print_info "Inserting test season data..."

    docker compose exec -T postgres psql -U mlb_admin -d mlb_games << 'EOF'
INSERT INTO season.seasons (request_params, source_url, data, sport_id, season_id)
VALUES (
  '{"sportId": 1}'::jsonb,
  'https://statsapi.mlb.com/api/v1/seasons?sportId=1',
  '{"seasons": [
    {"seasonId": "2024", "regularSeasonStartDate": "2024-03-20", "regularSeasonEndDate": "2024-09-29",
     "seasonStartDate": "2024-02-14", "seasonEndDate": "2024-11-02", "springStartDate": "2024-02-14",
     "springEndDate": "2024-03-19", "hasWildcard": true, "allStarDate": "2024-07-16",
     "qualifierPlateAppearances": 502.0, "qualifierOutsPitched": 486.0}
  ]}'::jsonb,
  1,
  '2024'
)
ON CONFLICT DO NOTHING;
EOF

    print_success "Test season data inserted"
}

# Insert test schedule data
insert_test_schedule_data() {
    print_info "Inserting test schedule data..."

    docker compose exec -T postgres psql -U mlb_admin -d mlb_games << 'EOF'
INSERT INTO schedule.schedule (request_params, source_url, data, sport_id, schedule_date)
VALUES (
  '{"sportId": 1, "date": "2024-07-04"}'::jsonb,
  'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=2024-07-04',
  '{"totalGames": 2, "totalEvents": 0, "totalItems": 2, "dates": [
    {"date": "2024-07-04", "totalGames": 2, "games": [
      {"gamePk": 744834, "gameType": "R", "season": "2024", "gameDate": "2024-07-04T17:10:00Z",
       "officialDate": "2024-07-04", "status": {"statusCode": "F", "detailedState": "Final"},
       "teams": {
         "away": {"team": {"id": 121, "name": "New York Mets"}, "leagueRecord": {"wins": 45, "losses": 46}, "score": 3},
         "home": {"team": {"id": 120, "name": "Washington Nationals"}, "leagueRecord": {"wins": 43, "losses": 48}, "score": 2}
       },
       "venue": {"id": 3309, "name": "Nationals Park"}, "doubleHeader": "N", "dayNight": "day", "scheduledInnings": 9
      },
      {"gamePk": 745123, "gameType": "R", "season": "2024", "gameDate": "2024-07-04T19:05:00Z",
       "officialDate": "2024-07-04", "status": {"statusCode": "F", "detailedState": "Final"},
       "teams": {
         "away": {"team": {"id": 147, "name": "New York Yankees"}, "leagueRecord": {"wins": 58, "losses": 34}, "score": 8},
         "home": {"team": {"id": 111, "name": "Boston Red Sox"}, "leagueRecord": {"wins": 44, "losses": 43}, "score": 2}
       },
       "venue": {"id": 3, "name": "Fenway Park"}, "doubleHeader": "N", "dayNight": "night", "scheduledInnings": 9
      }
    ]}
  ]}'::jsonb,
  1,
  '2024-07-04'
)
ON CONFLICT DO NOTHING;
EOF

    print_success "Test schedule data inserted"
}

# Run season transformation
test_season_transform() {
    print_header "Testing Season Transformation"

    # Check for data, insert if missing
    if ! check_postgres_data "season.seasons" "season.seasons"; then
        insert_test_season_data
    fi

    # Run transformation
    print_info "Running season transformation..."

    if docker compose run --rm spark \
        examples/spark_jobs/transform_season.py; then
        print_success "Season transformation completed successfully"
    else
        print_error "Season transformation failed"
        return 1
    fi
}

# Run schedule transformation
test_schedule_transform() {
    print_header "Testing Schedule Transformation"

    # Check for data, insert if missing
    if ! check_postgres_data "schedule.schedule" "schedule.schedule"; then
        insert_test_schedule_data
    fi

    # Set environment variables for date range
    export START_DATE="2024-07-01"
    export END_DATE="2024-07-31"

    # Run transformation
    print_info "Running schedule transformation (${START_DATE} to ${END_DATE})..."

    if docker compose run --rm \
        -e START_DATE="${START_DATE}" \
        -e END_DATE="${END_DATE}" \
        spark examples/spark_jobs/transform_schedule.py; then
        print_success "Schedule transformation completed successfully"
    else
        print_error "Schedule transformation failed"
        return 1
    fi
}

# Insert test game data
insert_test_game_data() {
    print_info "Inserting test game data..."

    docker compose exec -T postgres psql -U mlb_admin -d mlb_games << 'EOF'
INSERT INTO game.live_game_v1 (request_params, source_url, data, game_pk, game_date)
VALUES (
  '{"gamePk": 744834}'::jsonb,
  'https://statsapi.mlb.com/api/v1.1/game/744834/feed/live',
  '{"gamePk": 744834, "gameData": {"datetime": {"dateTime": "2024-07-04T17:10:00Z", "officialDate": "2024-07-04", "dayNight": "day"}, "status": {"abstractGameState": "Final", "codedGameState": "F", "detailedState": "Final", "statusCode": "F"}, "teams": {"away": {"id": 121, "name": "New York Mets"}, "home": {"id": 120, "name": "Washington Nationals"}}, "venue": {"id": 3309, "name": "Nationals Park"}, "weather": {"condition": "Sunny", "temp": "85", "wind": "10 mph"}}, "liveData": {"boxscore": {"teams": {"away": {"team": {"id": 121, "name": "New York Mets"}, "teamStats": {"batting": {"runs": 3, "hits": 8, "errors": 1, "leftOnBase": 6}}}, "home": {"team": {"id": 120, "name": "Washington Nationals"}, "teamStats": {"batting": {"runs": 2, "hits": 6, "errors": 0, "leftOnBase": 5}}}}}}}'::jsonb,
  744834,
  '2024-07-04'
)
ON CONFLICT DO NOTHING;
EOF

    print_success "Test game data inserted"
}

# Run game transformation
test_game_transform() {
    print_header "Testing Game Transformation"

    # Check for data, insert if missing
    if ! check_postgres_data "game.live_game_v1" "game.live_game_v1"; then
        insert_test_game_data
    fi

    # Set environment variables
    export GAME_PKS="744834"

    # Run transformation
    print_info "Running game transformation (game_pk=${GAME_PKS})..."

    if docker compose run --rm \
        -e GAME_PKS="${GAME_PKS}" \
        spark examples/spark_jobs/transform_game.py; then
        print_success "Game transformation completed successfully"
    else
        print_error "Game transformation failed"
        return 1
    fi
}

# Main script
main() {
    local test_type=${1:-all}

    print_header "MLB Data Platform - Transformation Tests"

    # Check prerequisites
    check_docker

    # Ensure services are running
    print_info "Ensuring PostgreSQL is running..."
    docker compose up -d postgres
    sleep 5

    # Build Spark image
    build_spark_image

    # Run tests based on argument
    case "$test_type" in
        season)
            test_season_transform
            ;;
        schedule)
            test_schedule_transform
            ;;
        game)
            test_game_transform
            ;;
        all)
            test_season_transform
            echo ""
            test_schedule_transform
            echo ""
            test_game_transform
            ;;
        *)
            print_error "Unknown test type: $test_type"
            echo "Usage: $0 [season|schedule|game|all]"
            exit 1
            ;;
    esac

    # Summary
    echo ""
    print_header "Test Summary"
    print_success "All transformations completed successfully!"
    print_info "View transformed data:"
    echo "  docker compose exec postgres psql -U mlb_admin -d mlb_games"
}

# Run main function
main "$@"
