-- V2: Venue Enrichment Schema
-- Creates: venue schema with detailed ballpark information
-- Supports: Coordinates, dimensions, park factors, weather, historical tracking
-- Run Date: 2024-11-10

-- ==============================================================================
-- VENUE SCHEMA
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS venue;

-- ==============================================================================
-- VENUES - Master venue table with comprehensive ballpark information
-- ==============================================================================

CREATE TABLE venue.venues (
    venue_id INT PRIMARY KEY,

    -- Basic Info
    name VARCHAR(100) NOT NULL,
    venue_name_official VARCHAR(150),
    active BOOLEAN DEFAULT TRUE,

    -- Location
    address VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(10),
    country VARCHAR(3) DEFAULT 'USA',

    -- Coordinates
    latitude NUMERIC(10, 7),
    longitude NUMERIC(10, 7),
    elevation INT,  -- feet above sea level
    timezone VARCHAR(50),

    -- Field Characteristics
    capacity INT,
    turf_type VARCHAR(50),  -- Natural Grass, Artificial Turf, Hybrid
    roof_type VARCHAR(50),  -- Open, Retractable, Fixed Dome, None
    surface_type VARCHAR(50),

    -- Outfield Dimensions (7 standard measurements in feet)
    -- From MLB API fieldInfo
    left_line INT,        -- Left field foul line (e.g., 310')
    left_field INT,       -- Left field (e.g., 330')
    left_center INT,      -- Left-center (e.g., 375')
    center_field INT,     -- Center field (e.g., 400')
    right_center INT,     -- Right-center (e.g., 375')
    right_field INT,      -- Right field (e.g., 330')
    right_line INT,       -- Right field foul line (e.g., 310')

    -- Wall Heights (feet)
    wall_height_left INT,
    wall_height_center INT,
    wall_height_right INT,
    wall_height_min INT,   -- Minimum wall height
    wall_height_max INT,   -- Maximum wall height (e.g., Green Monster = 37')

    -- Stadium Characteristics
    backstop_distance INT,        -- Distance from home to backstop (feet)
    foul_territory_size VARCHAR(20),  -- Small, Medium, Large
    fair_territory_sqft INT,      -- Square footage of fair territory

    -- Special Features (from MLB API + manual data)
    has_manual_scoreboard BOOLEAN DEFAULT FALSE,
    has_warning_track BOOLEAN DEFAULT TRUE,
    notable_features TEXT[],  -- ['Green Monster', 'Ivy', 'Monument Park', etc.]

    -- Data Source Tracking
    data_source VARCHAR(50) DEFAULT 'mlb_api',  -- mlb_api, seamheads, manual, etc.
    data_quality VARCHAR(20) DEFAULT 'verified',  -- verified, estimated, unverified
    last_verified_date DATE,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    notes TEXT
);

-- Indexes for venues table
CREATE INDEX idx_venues_name ON venue.venues(name);
CREATE INDEX idx_venues_city ON venue.venues(city);
CREATE INDEX idx_venues_active ON venue.venues(active);
CREATE INDEX idx_venues_coordinates ON venue.venues(latitude, longitude);

COMMENT ON TABLE venue.venues IS 'Master venue table with comprehensive ballpark information from MLB API and external sources';
COMMENT ON COLUMN venue.venues.left_line IS 'Left field foul line distance (feet) - from MLB API fieldInfo.leftLine';
COMMENT ON COLUMN venue.venues.center_field IS 'Center field distance (feet) - from MLB API fieldInfo.center';
COMMENT ON COLUMN venue.venues.data_source IS 'Source of dimension data: mlb_api, seamheads, clems_baseball, manual';

-- ==============================================================================
-- VENUE_HISTORY - Track dimensional changes over time
-- ==============================================================================

CREATE TABLE venue.venue_history (
    id BIGSERIAL PRIMARY KEY,
    venue_id INT NOT NULL REFERENCES venue.venues(venue_id),

    -- Time Period
    effective_date DATE NOT NULL,
    end_date DATE,
    season_start INT,  -- First season with these dimensions
    season_end INT,    -- Last season with these dimensions

    -- Dimensions (same structure as venues table)
    left_line INT,
    left_field INT,
    left_center INT,
    center_field INT,
    right_center INT,
    right_field INT,
    right_line INT,

    wall_height_left INT,
    wall_height_center INT,
    wall_height_right INT,

    -- Change Tracking
    change_reason VARCHAR(200),  -- 'Renovation', 'New Stadium', 'Rule Change', etc.
    change_description TEXT,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    data_source VARCHAR(50)
);

-- Indexes for venue_history table
CREATE INDEX idx_venue_history_venue ON venue.venue_history(venue_id, effective_date DESC);
CREATE INDEX idx_venue_history_season ON venue.venue_history(season_start, season_end);

COMMENT ON TABLE venue.venue_history IS 'Historical tracking of ballpark dimension changes (renovations, new stadiums)';

-- ==============================================================================
-- STATCAST_PARK_FACTORS - Park factors from Baseball Savant
-- ==============================================================================

CREATE TABLE venue.statcast_park_factors (
    id BIGSERIAL PRIMARY KEY,
    venue_id INT NOT NULL REFERENCES venue.venues(venue_id),

    -- Time Period
    season INT NOT NULL,
    as_of_date DATE NOT NULL,  -- When these factors were calculated

    -- Stat Type
    stat_type VARCHAR(50) NOT NULL,  -- 'HR', '2B', '3B', '1B', 'SO', 'BB', etc.

    -- Handedness
    batter_hand VARCHAR(5),  -- 'L', 'R', 'Both'
    pitcher_hand VARCHAR(5), -- 'L', 'R', 'Both'

    -- Park Factor (100 = league average)
    park_factor NUMERIC(5, 2) NOT NULL,  -- e.g., 115 = 15% more HRs than average

    -- Sample Size
    sample_size INT,  -- Number of events in sample
    games_played INT,

    -- Additional Context
    day_night VARCHAR(10),   -- 'Day', 'Night', 'Both'
    roof_status VARCHAR(20), -- 'Open', 'Closed', 'N/A'

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    data_source VARCHAR(50) DEFAULT 'baseball_savant',

    UNIQUE (venue_id, season, stat_type, batter_hand, pitcher_hand, day_night)
);

-- Indexes for statcast_park_factors table
CREATE INDEX idx_park_factors_venue_season ON venue.statcast_park_factors(venue_id, season DESC);
CREATE INDEX idx_park_factors_stat ON venue.statcast_park_factors(stat_type);
CREATE INDEX idx_park_factors_factor ON venue.statcast_park_factors(park_factor DESC);

COMMENT ON TABLE venue.statcast_park_factors IS 'Park factors from Baseball Savant Statcast data - how venue affects offensive statistics';
COMMENT ON COLUMN venue.statcast_park_factors.park_factor IS 'Park factor where 100 = league average. >100 favors hitters, <100 favors pitchers';

-- ==============================================================================
-- FENCE_COORDINATES - Detailed fence shape for visualization
-- ==============================================================================

CREATE TABLE venue.fence_coordinates (
    id BIGSERIAL PRIMARY KEY,
    venue_id INT NOT NULL REFERENCES venue.venues(venue_id),

    -- Time Period
    effective_date DATE NOT NULL,
    season INT,

    -- Coordinate Point
    angle NUMERIC(5, 2) NOT NULL,  -- Degrees from home plate (0° = center field, + = clockwise)
    distance_feet INT NOT NULL,    -- Distance from home plate to fence
    wall_height_feet INT,          -- Wall height at this point

    -- Description
    description TEXT,  -- e.g., 'Green Monster', 'Pesky's Pole', 'Death Valley'
    landmark BOOLEAN DEFAULT FALSE,  -- Is this a notable landmark?

    -- Data Source
    data_source VARCHAR(50),  -- 'fangraphs_equations', 'manual_measurement', 'cad_drawing'

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (venue_id, effective_date, angle)
);

-- Indexes for fence_coordinates table
CREATE INDEX idx_fence_coords_venue ON venue.fence_coordinates(venue_id, effective_date, angle);

COMMENT ON TABLE venue.fence_coordinates IS 'Detailed outfield fence coordinates for 3D visualization and spray chart overlays';
COMMENT ON COLUMN venue.fence_coordinates.angle IS 'Angle in degrees from home plate (0° = straightaway center field)';

-- ==============================================================================
-- VENUE_WEATHER - Historical weather conditions at venues
-- ==============================================================================

CREATE TABLE venue.venue_weather (
    id BIGSERIAL PRIMARY KEY,
    venue_id INT NOT NULL REFERENCES venue.venues(venue_id),

    -- Time
    observation_time TIMESTAMPTZ NOT NULL,
    game_pk BIGINT,  -- Optional: link to specific game

    -- Weather Conditions
    condition VARCHAR(50),  -- 'Sunny', 'Cloudy', 'Rain', 'Snow', 'Roof Closed', etc.
    temperature_f INT,      -- Temperature in Fahrenheit
    feels_like_f INT,
    humidity_percent INT,

    -- Wind
    wind_speed_mph INT,
    wind_direction VARCHAR(10),  -- 'N', 'NE', 'E', etc. or degrees
    wind_description TEXT,       -- 'Out to LF', 'In from CF', etc.

    -- Precipitation
    precipitation_in NUMERIC(4, 2),
    precipitation_type VARCHAR(20),  -- 'Rain', 'Snow', 'None'

    -- Atmospheric
    barometric_pressure_inhg NUMERIC(4, 2),
    visibility_miles NUMERIC(4, 1),

    -- Data Source
    data_source VARCHAR(50),  -- 'mlb_api', 'weather_underground', 'noaa'

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for venue_weather table
CREATE INDEX idx_venue_weather_venue_time ON venue.venue_weather(venue_id, observation_time DESC);
CREATE INDEX idx_venue_weather_game ON venue.venue_weather(game_pk);

COMMENT ON TABLE venue.venue_weather IS 'Historical weather data at ballparks - from MLB API game data and external weather services';

-- ==============================================================================
-- VENUE_METADATA - Additional venue metadata and enrichment
-- ==============================================================================

CREATE TABLE venue.venue_metadata (
    venue_id INT PRIMARY KEY REFERENCES venue.venues(venue_id),

    -- Team Information
    primary_tenant_team_id INT,
    tenant_teams INT[],  -- Array of team IDs that play here

    -- Historical
    opened_year INT,
    closed_year INT,
    architect VARCHAR(100),
    construction_cost_millions NUMERIC(10, 2),

    -- Classification
    generation VARCHAR(20),  -- 'Classic', 'Multi-purpose', 'Retro', 'Modern', etc.
    league VARCHAR(5),       -- 'AL', 'NL', 'Both'
    division VARCHAR(10),

    -- Records
    attendance_record INT,
    attendance_record_date DATE,

    -- URLs and Media
    official_website TEXT,
    wikipedia_url TEXT,
    image_urls TEXT[],

    -- GeoJSON for mapping (full outline)
    boundary_geojson JSONB,

    -- Additional Data
    amenities TEXT[],  -- ['Retractable Roof', 'Natural Grass', 'Monument Park']
    seating_sections JSONB,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE venue.venue_metadata IS 'Extended metadata for venues - history, records, media, geo data';

-- ==============================================================================
-- HELPER FUNCTIONS
-- ==============================================================================

-- Function to get active venues
CREATE OR REPLACE FUNCTION venue.get_active_venues()
RETURNS TABLE (
    venue_id INT,
    name VARCHAR(100),
    city VARCHAR(100),
    capacity INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT v.venue_id, v.name, v.city, v.capacity
    FROM venue.venues v
    WHERE v.active = TRUE
    ORDER BY v.name;
END;
$$ LANGUAGE plpgsql;

-- Function to get venue dimensions for a specific season
CREATE OR REPLACE FUNCTION venue.get_dimensions_for_season(
    p_venue_id INT,
    p_season INT
)
RETURNS TABLE (
    left_line INT,
    left_field INT,
    left_center INT,
    center_field INT,
    right_center INT,
    right_field INT,
    right_line INT
) AS $$
BEGIN
    -- First try to find historical dimensions for this season
    RETURN QUERY
    SELECT h.left_line, h.left_field, h.left_center, h.center_field,
           h.right_center, h.right_field, h.right_line
    FROM venue.venue_history h
    WHERE h.venue_id = p_venue_id
      AND p_season BETWEEN h.season_start AND COALESCE(h.season_end, 9999)
    LIMIT 1;

    -- If no historical record, fall back to current dimensions
    IF NOT FOUND THEN
        RETURN QUERY
        SELECT v.left_line, v.left_field, v.left_center, v.center_field,
               v.right_center, v.right_field, v.right_line
        FROM venue.venues v
        WHERE v.venue_id = p_venue_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate distance between two venues (Haversine formula)
CREATE OR REPLACE FUNCTION venue.calculate_distance_miles(
    lat1 NUMERIC, lon1 NUMERIC,
    lat2 NUMERIC, lon2 NUMERIC
)
RETURNS NUMERIC AS $$
DECLARE
    r NUMERIC := 3959;  -- Earth radius in miles
    dlat NUMERIC;
    dlon NUMERIC;
    a NUMERIC;
    c NUMERIC;
BEGIN
    dlat := radians(lat2 - lat1);
    dlon := radians(lon2 - lon1);

    a := sin(dlat/2) * sin(dlat/2) +
         cos(radians(lat1)) * cos(radians(lat2)) *
         sin(dlon/2) * sin(dlon/2);

    c := 2 * atan2(sqrt(a), sqrt(1-a));

    RETURN r * c;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION venue.calculate_distance_miles IS 'Calculate great-circle distance in miles between two coordinates using Haversine formula';

-- ==============================================================================
-- MATERIALIZED VIEWS
-- ==============================================================================

-- Current park factors by venue (most recent season)
CREATE MATERIALIZED VIEW venue.current_park_factors AS
SELECT DISTINCT ON (venue_id, stat_type)
    venue_id,
    v.name as venue_name,
    v.city,
    stat_type,
    park_factor,
    season,
    sample_size
FROM venue.statcast_park_factors pf
JOIN venue.venues v USING (venue_id)
WHERE batter_hand = 'Both' AND pitcher_hand = 'Both' AND day_night = 'Both'
ORDER BY venue_id, stat_type, season DESC;

CREATE INDEX ON venue.current_park_factors (venue_id);
CREATE INDEX ON venue.current_park_factors (stat_type);

-- Venue summary with latest dimensions and park factors
CREATE MATERIALIZED VIEW venue.venue_summary AS
SELECT
    v.venue_id,
    v.name,
    v.city,
    v.state,
    v.latitude,
    v.longitude,
    v.elevation,
    v.capacity,
    v.turf_type,
    v.roof_type,
    v.center_field,
    (SELECT park_factor FROM venue.statcast_park_factors
     WHERE venue_id = v.venue_id AND stat_type = 'HR'
     ORDER BY season DESC LIMIT 1) as hr_park_factor,
    v.active,
    v.data_quality
FROM venue.venues v
WHERE v.active = TRUE
ORDER BY v.name;

CREATE UNIQUE INDEX ON venue.venue_summary (venue_id);

-- ==============================================================================
-- SAMPLE DATA INSERT (for 30 MLB venues - to be populated from scraping)
-- ==============================================================================

-- Note: This will be populated by:
-- 1. Extracting from Game.liveGameV1() responses (venue.location, venue.fieldInfo)
-- 2. Scraping Baseball Savant for park factors
-- 3. Scraping Seamheads.com for historical dimensions
-- 4. Manual entry for special features and metadata

COMMENT ON SCHEMA venue IS 'Venue enrichment schema - comprehensive ballpark data for analysis and visualization';
