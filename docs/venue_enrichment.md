# Venue Enrichment

Comprehensive ballpark data enrichment for analysis and visualization.

## Overview

The venue enrichment module provides:

1. **Park Factors** from Baseball Savant (Statcast data)
2. **Historical Dimensions** from Seamheads.com
3. **Venue Metadata** from MLB Stats API
4. **Weather Data** integration for historical conditions
5. **Fence Coordinates** for 3D visualization and spray charts

## Database Schema

### Core Tables

#### `venue.venues`
Master venue table with comprehensive ballpark information:
- Basic info: name, location, coordinates
- Capacity and field characteristics
- Outfield dimensions (7 standard measurements)
- Wall heights
- Special features (Green Monster, Ivy, etc.)

#### `venue.statcast_park_factors`
Park factors from Baseball Savant showing how venues affect statistics:
- Season and rolling averages
- By stat type (HR, 2B, 3B, wOBA, etc.)
- By handedness (L/R/Both for batters and pitchers)
- By conditions (day/night, roof status)

Park factors are normalized: **100 = league average**
- Greater than 100 = favors hitters
- Less than 100 = favors pitchers

Example: Coors Field HR park factor = 134.8 (34.8% more home runs than average)

#### `venue.venue_history`
Historical tracking of ballpark dimension changes:
- Renovations
- New stadium configurations
- Rule changes affecting dimensions

#### `venue.fence_coordinates`
Detailed outfield fence shape for visualization:
- Angle in degrees from home plate (0° = center field)
- Distance at each angle
- Wall height at each point
- Notable landmarks (Pesky's Pole, Death Valley, etc.)

#### `venue.venue_weather`
Historical weather conditions at venues:
- Temperature, humidity, wind
- Precipitation
- Barometric pressure
- Link to specific games

#### `venue.venue_metadata`
Extended metadata:
- Team information
- Historical data (opened year, architect, construction cost)
- Classification (Classic, Retro, Modern, etc.)
- Records and media URLs

### Helper Functions

#### `venue.get_active_venues()`
Returns list of currently active MLB venues.

#### `venue.get_dimensions_for_season(venue_id, season)`
Returns dimensions for a specific venue and season, accounting for historical changes.

#### `venue.calculate_distance_miles(lat1, lon1, lat2, lon2)`
Haversine formula for calculating great-circle distance between coordinates.

### Materialized Views

#### `venue.current_park_factors`
Latest park factors for each venue and stat type.

#### `venue.venue_summary`
Venue summary with latest dimensions and HR park factor.

## Data Sources

### 1. Baseball Savant (Statcast Park Factors)

**Source**: https://baseballsavant.mlb.com/leaderboard/statcast-park-factors

**Priority**: High - Statcast data is authoritative and updated regularly

**Data Points**:
- Park factors for HR, 2B, 3B, 1B, wOBA, etc.
- Single-season and rolling averages (3-year)
- By handedness and day/night splits

**Update Frequency**: After each season, updated mid-season

**Implementation Status**: ✓ Scraper complete, CLI integration complete

### 2. Seamheads.com (Historical Dimensions)

**Source**: https://www.seamheads.com/ballparks/

**Priority**: Medium - Historical dimension changes for analysis

**Data Points**:
- Outfield distances over time
- Wall heights
- Renovation history
- Opening/closing dates

**Update Frequency**: Historical data, updated infrequently

**Implementation Status**: ✓ Scraper complete, CLI integration complete

### 3. MLB Stats API (Venue Metadata)

**Source**: Game.liveGameV1() responses include venue information

**Priority**: High - Official source for current venue data

**Data Points**:
- Venue ID, name, location
- Coordinates (lat/lon)
- Basic field info

**Update Frequency**: Real-time with game data

**Implementation Status**: In progress - extraction logic being developed

### 4. Clem's Baseball (Power Alleys)

**Source**: http://www.andrewclem.com/Baseball/

**Priority**: Low - Supplemental data for precision

**Data Points**:
- Power alley estimates
- Detailed fence equations

**Update Frequency**: Historical data

**Implementation Status**: Future consideration

### 5. FanGraphs (Piecewise Fence Equations)

**Source**: Various FanGraphs park dimensions articles

**Priority**: Low - Mathematical fence shapes

**Data Points**:
- Piecewise linear equations for fence shape
- Equations for spray chart overlays

**Update Frequency**: Occasional updates

**Implementation Status**: Future consideration

## CLI Usage

### Fetch Park Factors

Scrape park factors from Baseball Savant and save to database:

```bash
# Fetch 2024 season park factors
uv run mlb-etl venue fetch-park-factors --season 2024

# Fetch 3-year rolling average
uv run mlb-etl venue fetch-park-factors --season 2024 --rolling 3

# Specify different database
uv run mlb-etl venue fetch-park-factors \
  --season 2024 \
  --db-host localhost \
  --db-port 65254
```

### Fetch Seamheads Dimensions

Scrape historical ballpark dimensions from Seamheads.com:

```bash
# Fetch all active MLB ballparks
uv run mlb-etl venue fetch-seamheads

# Fetch all ballparks (including historical)
uv run mlb-etl venue fetch-seamheads --all

# Fetch specific ballpark by Seamheads park ID
uv run mlb-etl venue fetch-seamheads --park-id BOS07  # Fenway Park

# Common Seamheads park IDs:
# BOS07 - Fenway Park
# NYA03 - Yankee Stadium
# CHN02 - Wrigley Field
# LOS03 - Dodger Stadium
# DEN02 - Coors Field
```

### List Venues

Show all MLB venues from database:

```bash
uv run mlb-etl venue list-venues
```

Output:
```
┌─────┬────────────────────┬─────────────┬───────┬──────────┬─────────────┐
│  ID │ Name               │ City        │ State │ Capacity │ CF Distance │
├─────┼────────────────────┼─────────────┼───────┼──────────┼─────────────┤
│   2 │ Fenway Park        │ Boston      │ MA    │   37,755 │        420' │
│  19 │ Coors Field        │ Denver      │ CO    │   50,144 │        415' │
│  22 │ Dodger Stadium     │ Los Angeles │ CA    │   56,000 │        395' │
└─────┴────────────────────┴─────────────┴───────┴──────────┴─────────────┘
Total venues: 30
```

### List Park Factors

Query saved park factors:

```bash
# All park factors for 2024
uv run mlb-etl venue list-park-factors --season 2024

# Filter by stat type (home runs)
uv run mlb-etl venue list-park-factors --season 2024 --stat-type HR

# Filter by venue
uv run mlb-etl venue list-park-factors --venue-id 2 --season 2024
```

### Refresh Materialized Views

After loading new park factors, refresh the materialized views:

```bash
uv run mlb-etl venue refresh-views
```

## Python API Usage

### Scraping Park Factors (Baseball Savant)

```python
from mlb_data_platform.venue import BaseballSavantScraper

# Create scraper with context manager (auto-closes HTTP client)
with BaseballSavantScraper() as scraper:
    # Fetch 2024 single-season park factors
    factors_2024 = scraper.fetch_park_factors(season=2024)

    # Fetch 3-year rolling average
    factors_3yr = scraper.fetch_park_factors(season=2024, rolling=3)

# Access park factor data
for factor in factors_2024:
    print(f"{factor.venue_name}: {factor.stat_type} = {factor.park_factor}")
```

### Scraping Dimensions (Seamheads)

```python
from mlb_data_platform.venue import SeamheadsScraper

# Create scraper with context manager
with SeamheadsScraper() as scraper:
    # Fetch specific ballpark by Seamheads park ID
    fenway = scraper.fetch_ballpark_by_id("BOS07")
    print(f"{fenway.name}: CF = {fenway.dimensions.center_field}'")

    # Fetch all active MLB ballparks
    active_venues = scraper.fetch_all_ballparks(active_only=True)

    # Fetch all ballparks (including historical)
    all_venues = scraper.fetch_all_ballparks(active_only=False)

# Access venue data
print(f"Name: {fenway.name}")
print(f"City: {fenway.city}, {fenway.state}")
print(f"Capacity: {fenway.capacity:,}")
print(f"Coordinates: {fenway.latitude}, {fenway.longitude}")
print(f"Elevation: {fenway.elevation} ft")
if fenway.dimensions:
    print(f"Left: {fenway.dimensions.left_field}'")
    print(f"Center: {fenway.dimensions.center_field}'")
    print(f"Right: {fenway.dimensions.right_field}'")
```

### Saving to Database

```python
from mlb_data_platform.venue import BaseballSavantScraper, VenueStorageBackend
from mlb_data_platform.storage.postgres import PostgresConfig, PostgresStorageBackend

# Setup database connection
pg_config = PostgresConfig(
    host="localhost",
    port=65254,
    database="mlb_games",
    user="mlb_admin",
    password="mlb_admin_password",
)
postgres = PostgresStorageBackend(pg_config)
venue_storage = VenueStorageBackend(postgres)

# Scrape and save
with BaseballSavantScraper() as scraper:
    park_factors = scraper.fetch_park_factors(season=2024)

    # Upsert to database (handles duplicates)
    count = venue_storage.upsert_park_factors(park_factors)
    print(f"Saved {count} park factors")

    # Refresh materialized views
    venue_storage.refresh_materialized_views()
```

### Querying Park Factors

```python
# Get all park factors for Fenway Park
factors = venue_storage.get_park_factors(venue_id=2)

# Get HR park factors for 2024
hr_factors = venue_storage.get_park_factors(season=2024, stat_type="HR")

# Get all venues
venues = venue_storage.get_venues(active_only=True)
```

## Implementation Notes

### Baseball Savant Scraping Strategy

Baseball Savant serves data via JavaScript-rendered tables. The scraper uses multiple strategies to extract data:

1. **JavaScript Variable Extraction**: Parse embedded `var data = [...]` in `<script>` tags
2. **Data Attributes**: Extract from `data-json` or `data-table` HTML attributes
3. **CSV Download**: Detect and fetch CSV export links if available

The scraper includes:
- Retry logic with exponential backoff (using `tenacity`)
- Data validation to ensure correct format
- Flexible field mapping to handle Baseball Savant's naming conventions
- Comprehensive error handling and logging

### Data Quality Tracking

All venue data includes quality metadata:
- `data_source`: Origin of data (mlb_api, baseball_savant, seamheads, manual)
- `data_quality`: Assessment (verified, estimated, unverified)
- `last_verified_date`: When data was last confirmed accurate
- `as_of_date`: When park factors were calculated

### Foreign Key Relationships

All venue tables have foreign keys to `venue.venues(venue_id)`:
- `venue.statcast_park_factors`
- `venue.venue_history`
- `venue.fence_coordinates`
- `venue.venue_weather`
- `venue.venue_metadata`

This ensures referential integrity and enables JOIN queries.

### Index Strategy

Indexes are created for:
1. **Foreign keys**: All `venue_id` columns
2. **Time-based queries**: `season`, `captured_at`, `effective_date`
3. **Filter columns**: `stat_type`, `active`, `city`
4. **Sort columns**: `park_factor`, `name`
5. **Composite indexes**: `(venue_id, season)`, `(latitude, longitude)`

## Implementation Roadmap

### Phase 1: Basic Enrichment (✅ Complete)
- ✓ Database schema (V2 migration)
- ✓ Baseball Savant park factors scraper
- ✓ Seamheads.com ballpark dimensions scraper
- ✓ Storage backend with UPSERT logic
- ✓ CLI integration for both scrapers

### Phase 2: MLB API Integration (In Progress)
- Extract venue data from Game.liveGameV1() responses
- Map Seamheads park IDs to MLB venue IDs
- Create enrichment job combining all data sources
- Automated venue data updates

### Phase 3: Advanced Visualization (Future)
- Fence coordinate generation
- 3D ballpark visualization
- Spray chart overlays
- Weather data integration
- Historical dimension tracking and animation

## Testing

Unit tests are located in `tests/unit/test_venue_scraper.py`.

Run tests:
```bash
# All venue tests
pytest tests/unit/test_venue_scraper.py -v

# Specific test
pytest tests/unit/test_venue_scraper.py::TestBaseballSavantScraper::test_fetch_park_factors_success -v

# With coverage
pytest tests/unit/test_venue_scraper.py --cov=mlb_data_platform.venue --cov-report=html
```

## References

- **Baseball Savant Park Factors**: https://baseballsavant.mlb.com/leaderboard/statcast-park-factors
- **Seamheads Ballparks**: https://www.seamheads.com/ballparks/
- **MLB Stats API**: https://statsapi.mlb.com/docs/
- **Park Factor Methodology**: https://library.fangraphs.com/principles/park-factors/

---

**Status**: Phase 1 complete! Ready for production use with Baseball Savant park factors and Seamheads ballpark dimensions.
**Last Updated**: 2025-01-10
**Next Steps**: Extract venue data from MLB API Game responses and create automated enrichment jobs.
