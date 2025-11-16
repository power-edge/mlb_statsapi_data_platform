# MLB Stats API Data Platform - POC Backlog

**Purpose**: Track proof-of-concept user stories and features for demonstrating platform capabilities.

---

## 🎯 Primary POC Goals

1. **Multi-Tenant Data Mart Access** - Customers can query MLB data via Apache Superset
2. **Team Travel Visualization** - Visualize team travel patterns across season with distance/type breakdowns
3. **Schedule Optimization** - Model and optimize MLB schedule balancing rules and travel
4. **Ballpark Analysis** - Analyze ballpark factors, dimensions, weather impacts on games

---

## 📊 POC User Stories

### Epic 1: Multi-Tenant Apache Superset Integration

#### Story 1.1: Customer Data Mart Provisioning
**As a** platform administrator
**I want to** provision isolated database access for customers
**So that** each customer can only see their authorized data subset

**Acceptance Criteria:**
- [ ] PostgreSQL role template system working
- [ ] Script to create customer-specific roles with scoped permissions
- [ ] Customer can connect to PostgreSQL with their credentials
- [ ] Customer can only SELECT from authorized schemas
- [ ] Audit logging of customer queries

**Technical Tasks:**
- [ ] Implement `scripts/provision_customer.sh`
- [ ] Create SQL role template with base permissions
- [ ] Design customer tier system (free, basic, premium, enterprise)
- [ ] Add row-level security for multi-tenancy (if needed)
- [ ] Create customer metadata tracking table

---

#### Story 1.2: Apache Superset Environment Setup
**As a** customer
**I want to** access a personal Apache Superset instance
**So that** I can create custom dashboards and visualizations

**Acceptance Criteria:**
- [ ] Superset deployed in Kubernetes (one instance per customer OR shared with RBAC)
- [ ] Customer can login with SSO or dedicated credentials
- [ ] PostgreSQL data source pre-configured for customer
- [ ] Sample dashboards provided as templates
- [ ] Customer can create/edit/delete their own dashboards

**Technical Tasks:**
- [ ] Create Helm chart for Apache Superset
- [ ] Design multi-tenant strategy (shared vs isolated instances)
- [ ] Configure PostgreSQL connection with customer-specific credentials
- [ ] Create starter dashboard templates:
  - Team performance dashboard
  - Player stats dashboard
  - Game outcomes dashboard
  - Venue/travel analysis dashboard
- [ ] Document Superset deployment and customer onboarding

---

### Epic 2: Team Travel Visualization & Analysis

#### Story 2.1: Venue Location Data Enrichment
**As an** analyst
**I want to** have accurate lat/long coordinates for all MLB venues
**So that** I can calculate travel distances between games

**Acceptance Criteria:**
- [ ] All venues have lat/long coordinates stored
- [ ] Coordinates validated against known sources
- [ ] Venue timezone information captured
- [ ] Elevation data available
- [ ] Address information complete (city, state, zip)

**Technical Tasks:**
- [x] Extract venue data from `liveGameV1` responses (DONE - see venue structure above)
- [x] Research detailed ballpark dimension sources (DONE - see research notes below)
- [ ] Create `venue.venues` normalized table with:
  ```sql
  CREATE TABLE venue.venues (
      venue_id INT PRIMARY KEY,
      name VARCHAR(100),
      address VARCHAR(200),
      city VARCHAR(100),
      state VARCHAR(50),
      postal_code VARCHAR(10),
      country VARCHAR(3),
      latitude NUMERIC(10, 7),
      longitude NUMERIC(10, 7),
      elevation INT,  -- feet
      timezone VARCHAR(50),
      -- Field dimensions
      capacity INT,
      turf_type VARCHAR(50),
      roof_type VARCHAR(50),
      left_line INT,
      left_field INT,
      left_center INT,
      center_field INT,
      right_center INT,
      right_field INT,
      right_line INT
  );
  ```
- [ ] Ingest venue data from Venue API endpoint
- [ ] Create `venue.venue_history` for tracking changes over time (renovations, etc.)

---

#### Story 2.2: Team Travel Distance Calculation
**As an** analyst
**I want to** calculate the distance each team travels between games
**So that** I can analyze travel burden and fairness

**Acceptance Criteria:**
- [ ] Distance calculated using Haversine formula (great-circle distance)
- [ ] Distances stored in miles and kilometers
- [ ] Travel calculated game-to-game for each team
- [ ] Cumulative travel distance by team/season
- [ ] Travel distance by season type (preseason, regular, postseason)

**Technical Tasks:**
- [ ] Create `analytics.team_travel` table:
  ```sql
  CREATE TABLE analytics.team_travel (
      id BIGSERIAL PRIMARY KEY,
      team_id INT NOT NULL,
      season VARCHAR(4) NOT NULL,
      season_type VARCHAR(10),  -- preseason, regular, postseason
      from_game_pk BIGINT,
      to_game_pk BIGINT,
      from_venue_id INT,
      to_venue_id INT,
      from_date DATE,
      to_date DATE,
      distance_miles NUMERIC(10, 2),
      distance_km NUMERIC(10, 2),
      days_between_games INT,
      INDEX idx_team_season (team_id, season),
      INDEX idx_season_type (season_type)
  );
  ```
- [ ] Implement Haversine distance calculation (Python/SQL function)
- [ ] Create PySpark job to calculate all team travel distances
- [ ] Create materialized view for cumulative travel by team/season
- [ ] Add to nightly batch workflow

---

#### Story 2.3: Interactive Travel Map Visualization
**As an** analyst
**I want to** visualize team travel on an interactive map
**So that** I can see geographic patterns and identify imbalances

**Acceptance Criteria:**
- [ ] Map shows all MLB venue locations
- [ ] User can select a team to highlight their travel
- [ ] Lines drawn between consecutive games
- [ ] Line color/thickness indicates season type or distance
- [ ] Tooltip shows game info, distance, days rest
- [ ] Cumulative statistics panel (total miles, avg distance per trip)
- [ ] Filter by season year and season type

**Technical Tasks:**
- [ ] Create Superset deck.gl visualization OR custom React dashboard
- [ ] Query to get team schedule with venue coordinates
- [ ] GeoJSON format for map rendering
- [ ] Add interactivity (team selector, season filter)
- [ ] Deploy as sample dashboard in customer Superset

**Visual Mockup:**
```
[Map of USA]
  • Venue markers (30 MLB stadiums)
  • Lines connecting consecutive games for selected team
  • Color coding:
    - Blue: Preseason
    - Green: Regular season
    - Red: Postseason
  • Stats panel:
    - Total miles: 45,234
    - Longest trip: 2,834 miles (Seattle → Miami)
    - Average distance: 1,203 miles
    - Games at home: 81
    - Games away: 81
```

---

### Epic 3: Ballpark Dimensions & Weather Analysis

#### Story 3.1: Ballpark Dimensions Database
**As an** analyst
**I want to** query ballpark dimensions for all venues
**So that** I can analyze how field size affects game outcomes

**Acceptance Criteria:**
- [x] All MLB ballpark dimensions captured (DONE - in venue.fieldInfo)
- [ ] Dimensions stored in normalized table
- [ ] Historical dimensions tracked (renovations change fields)
- [ ] Derived metrics calculated (total outfield area, symmetry index)
- [ ] "Ballpark factor" calculations (how park affects offense/defense)

**Technical Tasks:**
- [ ] Create `venue.field_dimensions` table with history:
  ```sql
  CREATE TABLE venue.field_dimensions (
      id BIGSERIAL PRIMARY KEY,
      venue_id INT NOT NULL,
      effective_date DATE NOT NULL,
      capacity INT,
      turf_type VARCHAR(50),
      roof_type VARCHAR(50),
      -- Outfield distances (feet)
      left_line INT,
      left_field INT,
      left_center INT,
      center_field INT,
      right_center INT,
      right_field INT,
      right_line INT,
      -- Derived metrics
      total_outfield_area INT,  -- Calculated polygon area
      symmetry_index NUMERIC(5, 2),  -- How symmetric L vs R
      average_depth INT,  -- Average outfield depth
      UNIQUE (venue_id, effective_date)
  );
  ```
- [ ] Calculate derived metrics (area, symmetry)
- [ ] Research historical ballpark changes
- [ ] Create "ballpark factor" calculation methodology
- [ ] Implement as materialized view or scheduled calculation

**Ballpark Factor Formula Ideas:**
- Compare team batting avg at home vs away
- Compare HR rates at each park vs league average
- Adjust for weather, elevation, prevailing wind
- Create separate factors for LHB vs RHB

---

#### Story 3.2: Weather Data Integration
**As an** analyst
**I want to** have weather data for each game
**So that** I can analyze weather impacts on performance

**Acceptance Criteria:**
- [x] Game-time weather captured from liveGameV1 (DONE - condition, temp, wind)
- [ ] Weather stored in normalized table
- [ ] Historical weather data backfilled (if available)
- [ ] Weather categorized (clear, cloudy, rain, snow, dome/roof closed)
- [ ] Wind speed and direction parsed
- [ ] Temperature in Fahrenheit and Celsius

**Technical Tasks:**
- [ ] Create `game.weather` table:
  ```sql
  CREATE TABLE game.weather (
      id BIGSERIAL PRIMARY KEY,
      game_pk BIGINT NOT NULL REFERENCES game.live_game_metadata(game_pk),
      captured_at TIMESTAMPTZ NOT NULL,
      condition VARCHAR(100),  -- "Clear", "Roof Closed", "Cloudy", etc.
      temperature_f INT,
      temperature_c INT,
      wind_speed_mph INT,
      wind_direction VARCHAR(10),  -- "N", "NE", "E", "SE", etc.
      is_dome BOOLEAN,
      is_roof_closed BOOLEAN,
      INDEX idx_game_pk (game_pk)
  );
  ```
- [ ] Parse weather string ("78°F, Wind 10 mph NW")
- [ ] Categorize conditions (clear, overcast, rain, snow, dome)
- [ ] Research external weather API for backfilling historical data
- [ ] Consider real-time weather updates during games (API integration)

**Potential Weather APIs:**
- OpenWeatherMap (historical data available)
- Weather Underground (detailed historical)
- NOAA (free, US government data)

---

### Epic 4: Schedule Optimization & Rule Modeling

#### Story 4.1: MLB Scheduling Rules Documentation
**As a** data engineer
**I want to** document MLB scheduling rules by year
**So that** I can model constraints for schedule optimization

**Acceptance Criteria:**
- [ ] Rules documented in structured format (YAML or JSON)
- [ ] Rules versioned by year (rules change over time)
- [ ] Rules categorized (division, interleague, rest days, special series)
- [ ] References to official MLB sources included

**Technical Tasks:**
- [ ] Research MLB scheduling rules:
  - Division games (# of games vs each division opponent)
  - Interleague play rules
  - Rest day requirements (e.g., no more than 20 consecutive days)
  - Special series (London, Mexico, Field of Dreams, etc.)
  - All-Star break placement
  - Doubleheader rules
  - Season length (162 games per team)
- [ ] Create `config/rules/mlb_schedule_rules.yaml`:
  ```yaml
  version: "2024"
  effective_date: "2024-01-01"

  rules:
    season_length:
      total_games: 162
      home_games: 81
      away_games: 81

    division_games:
      same_division:
        games_per_opponent: 13  # 4 opponents × 13 = 52 games
        total: 52

    interdivision_games:
      same_league_different_division:
        games_per_opponent: 6  # or 7, varies
        total: 66  # 10 opponents × 6-7

    interleague_games:
      games_per_opponent: 3  # or 4
      total: 44  # varies by year

    rest_days:
      max_consecutive_games: 20
      all_star_break:
        min_days: 3
        max_days: 4

    special_series:
      international:
        - London
        - Mexico City
      special_events:
        - Field of Dreams
        - Little League Classic
  ```
- [ ] Store rules in database:
  ```sql
  CREATE TABLE metadata.schedule_rules (
      id SERIAL PRIMARY KEY,
      version VARCHAR(10) NOT NULL,
      effective_date DATE NOT NULL,
      rule_category VARCHAR(50) NOT NULL,
      rule_data JSONB NOT NULL,
      source_url TEXT,
      notes TEXT
  );
  ```

**Research Sources:**
- MLB official announcements
- Baseball Reference
- MLB CBA (Collective Bargaining Agreement)
- Historical schedule analysis

---

#### Story 4.2: Schedule Constraint Representation System
**As a** data scientist
**I want to** represent scheduling constraints programmatically
**So that** I can use them in optimization models

**Acceptance Criteria:**
- [ ] Constraints represented as Python classes
- [ ] Constraints can be validated against a proposed schedule
- [ ] Constraints can be serialized to/from JSON
- [ ] Constraints have priority/weight (hard vs soft)
- [ ] Constraint violations can be reported

**Technical Tasks:**
- [ ] Create constraint modeling system:
  ```python
  # src/mlb_data_platform/scheduling/constraints.py

  from abc import ABC, abstractmethod

  class ScheduleConstraint(ABC):
      def __init__(self, name: str, priority: str = "hard"):
          self.name = name
          self.priority = priority  # "hard" or "soft"

      @abstractmethod
      def validate(self, schedule) -> list[str]:
          """Return list of violations (empty if valid)."""
          pass

  class DivisionGamesConstraint(ScheduleConstraint):
      def __init__(self, games_per_opponent: int = 13):
          super().__init__("division_games", priority="hard")
          self.games_per_opponent = games_per_opponent

      def validate(self, schedule):
          violations = []
          # Check each team plays correct # vs division opponents
          for team in schedule.teams:
              for opponent in team.division_opponents:
                  games = schedule.count_games(team, opponent)
                  if games != self.games_per_opponent:
                      violations.append(
                          f"{team} vs {opponent}: {games} games (expected {self.games_per_opponent})"
                      )
          return violations

  class MaxConsecutiveGamesConstraint(ScheduleConstraint):
      def __init__(self, max_games: int = 20):
          super().__init__("max_consecutive_games", priority="hard")
          self.max_games = max_games

      def validate(self, schedule):
          violations = []
          for team in schedule.teams:
              consecutive = schedule.get_max_consecutive_games(team)
              if consecutive > self.max_games:
                  violations.append(
                      f"{team}: {consecutive} consecutive games (max {self.max_games})"
                  )
          return violations

  class TravelDistanceConstraint(ScheduleConstraint):
      def __init__(self, max_total_miles: int = 50000):
          super().__init__("max_travel_distance", priority="soft")
          self.max_total_miles = max_total_miles

      def validate(self, schedule):
          violations = []
          for team in schedule.teams:
              total_miles = schedule.calculate_total_travel(team)
              if total_miles > self.max_total_miles:
                  violations.append(
                      f"{team}: {total_miles} miles (max {self.max_total_miles})"
                      )
          return violations
  ```
- [ ] Create constraint loader from rules YAML
- [ ] Implement schedule validation engine
- [ ] Add constraint violation reporting

---

#### Story 4.3: Schedule Optimization Model
**As a** data scientist
**I want to** generate optimized MLB schedules
**So that** I can demonstrate improved fairness and reduced travel

**Acceptance Criteria:**
- [ ] Model respects all hard constraints (division games, season length, etc.)
- [ ] Model optimizes soft constraints (minimize travel, balance home/away streaks)
- [ ] Model can generate multiple schedule alternatives
- [ ] Generated schedules can be compared to actual MLB schedules
- [ ] Optimization results include fairness metrics

**Technical Tasks:**
- [ ] Choose optimization framework:
  - OR-Tools (Google)
  - PuLP (Python linear programming)
  - Gurobi (commercial, powerful)
  - Custom genetic algorithm
- [ ] Create optimization model:
  ```python
  # src/mlb_data_platform/scheduling/optimizer.py

  from ortools.sat.python import cp_model

  class MLBScheduleOptimizer:
      def __init__(self, constraints: list[ScheduleConstraint]):
          self.model = cp_model.CpModel()
          self.constraints = constraints

      def generate_schedule(
          self,
          teams: list[str],
          start_date: date,
          end_date: date,
      ) -> Schedule:
          # Decision variables
          # For each (team1, team2, date): is there a game?
          games = {}
          for date in date_range(start_date, end_date):
              for team1 in teams:
                  for team2 in teams:
                      if team1 != team2:
                          var = self.model.NewBoolVar(f"game_{team1}_{team2}_{date}")
                          games[(team1, team2, date)] = var

          # Hard constraints
          self._add_season_length_constraint(teams, games)
          self._add_division_games_constraint(teams, games)
          self._add_home_away_balance_constraint(teams, games)
          self._add_rest_day_constraints(teams, games)

          # Objective: Minimize total travel distance
          self._add_travel_minimization_objective(teams, games)

          # Solve
          solver = cp_model.CpSolver()
          status = solver.Solve(self.model)

          if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
              return self._extract_schedule(solver, games)
          else:
              raise ValueError("No feasible schedule found")
  ```
- [ ] Implement schedule comparison metrics:
  - Total travel distance (optimized vs actual)
  - Travel distance variance (fairness between teams)
  - Home/away streak lengths
  - Rest day distribution
- [ ] Create visualization comparing schedules
- [ ] Document optimization approach and results

**Optimization Objectives (Multi-Objective):**
1. **Minimize total travel** - Sum of all team travel distances
2. **Minimize travel variance** - Fairness between teams
3. **Maximize rest days** - Avoid long consecutive game streaks
4. **Balance home/away** - Avoid long home or away stretches

---

### Epic 5: POC Dashboards & Demonstrations

#### Story 5.1: Team Travel Dashboard
**As a** customer
**I want to** view team travel statistics and maps
**So that** I can analyze schedule fairness

**Dashboard Includes:**
- [ ] Interactive map (team selector, season filter)
- [ ] Travel statistics table (by team)
- [ ] Charts:
  - Total miles by team (bar chart)
  - Cumulative travel over season (line chart)
  - Travel by season type (stacked bar)
  - Longest trips (horizontal bar)
- [ ] Export to PDF/PNG

---

#### Story 5.2: Ballpark Factor Dashboard
**As a** customer
**I want to** compare ballpark factors across venues
**So that** I can understand how venues affect performance

**Dashboard Includes:**
- [ ] Ballpark dimensions comparison (radar chart)
- [ ] Ballpark factors by venue (table with sortable columns)
- [ ] Home run rates by park (bar chart)
- [ ] Weather impact analysis (scatter plots)
- [ ] Venue selector to drill down

---

#### Story 5.3: Schedule Optimization Comparison
**As a** customer
**I want to** compare optimized schedules to actual MLB schedules
**So that** I can see potential improvements

**Dashboard Includes:**
- [ ] Side-by-side comparison (actual vs optimized)
- [ ] Metrics comparison table
- [ ] Travel reduction visualization
- [ ] Fairness improvement metrics
- [ ] Interactive schedule calendar

---

## 🗺️ Implementation Roadmap

### Phase 1: Foundation (Week 1-2)
- [x] Venue data extraction from liveGameV1
- [ ] Create venue normalized tables
- [ ] Implement Haversine distance calculation
- [ ] Create team travel calculation job

### Phase 2: Visualization (Week 3-4)
- [ ] Deploy Apache Superset in K8s
- [ ] Create multi-tenant customer provisioning
- [ ] Build team travel map visualization
- [ ] Create travel statistics dashboard

### Phase 3: Advanced Analytics (Week 5-6)
- [ ] Research and document MLB scheduling rules
- [ ] Implement constraint representation system
- [ ] Build ballpark dimensions database
- [ ] Create ballpark factor calculations

### Phase 4: Optimization (Week 7-8)
- [ ] Implement schedule optimization model
- [ ] Generate alternative schedules
- [ ] Create comparison dashboards
- [ ] Document methodology and results

### Phase 5: POC Demonstration (Week 9-10)
- [ ] Finalize all dashboards
- [ ] Create demo data sets
- [ ] Prepare presentation materials
- [ ] Conduct POC demo with stakeholders

---

## 📚 Research Tasks

### Immediate Research Needed

1. **MLB Scheduling Rules** (Priority: High)
   - [ ] 2024 MLB schedule rules documentation
   - [ ] Historical rule changes (2010-present)
   - [ ] Special series requirements
   - [ ] Rest day mandates

2. **Ballpark Dimensions History** (Priority: Medium)
   - [ ] Renovations that changed field dimensions
   - [ ] New stadiums (2010-present)
   - [ ] Retired stadiums

3. **Weather Data Sources** (Priority: Medium)
   - [ ] Historical weather APIs
   - [ ] Real-time weather during games
   - [ ] Cost/access for historical data

4. **Ballpark Factor Methodologies** (Priority: Medium)
   - [ ] Literature review (sabermetrics papers)
   - [ ] Existing ballpark factor databases
   - [ ] Calculation methodologies

---

## 🎯 Success Metrics for POC

1. **Technical Success:**
   - [ ] 30 MLB venues with complete location/dimension data
   - [ ] Travel distances calculated for all teams, all seasons (2020-2024)
   - [ ] Apache Superset deployed with 3+ sample dashboards
   - [ ] Schedule optimization model generates feasible schedules
   - [ ] 95%+ uptime for data platform during POC period

2. **User Success:**
   - [ ] 5+ customers onboarded with Superset access
   - [ ] 10+ custom dashboards created by customers
   - [ ] Positive feedback on data quality and completeness
   - [ ] At least 1 "aha moment" insight discovered by customer

3. **Business Success:**
   - [ ] Demonstrated 10%+ reduction in total travel with optimized schedules
   - [ ] Identified measurable ballpark factors for all venues
   - [ ] Compelling visualizations for marketing materials
   - [ ] Clear value proposition for paid tiers

---

## 📝 Notes & Ideas

### Additional POC Ideas
- **Player performance by ballpark** - How players perform at different venues
- **Umpire analysis** - Which umpires have largest strike zones, most ejections, etc.
- **Injury correlation with travel** - Do teams with more travel have more injuries?
- **Home field advantage quantification** - How much does home field matter by team?
- **Optimal lineup construction** - Given ballpark factors and matchups
- **Real-time game prediction** - ML model predicting win probability during games

### External Data Integration Ideas
- **Weather Underground** - Historical game-time weather
- **Statcast** - Pitch-by-pitch data (if accessible)
- **Baseball Savant** - Advanced metrics
- **Retrosheet** - Historical game data (back to 1871)

---

## 🏟️ Ballpark Dimension Data Sources - Research Summary

### Current Limitations
The MLB Stats API provides **7 discrete outfield distance measurements** (leftLine, left, leftCenter, center, rightCenter, right, rightLine), which gives a basic approximation but **does not fully represent the true shape** of outfield walls. Real ballparks have:
- Complex curved sections between measured points
- Varying wall heights along the fence
- Unique irregularities (e.g., Fenway's Green Monster, Tropicana's catwalks)

### Recommended Data Sources

#### 1. **Baseball Savant - Statcast Park Factors** (Highest Priority)
- **URL**: https://baseballsavant.mlb.com/leaderboard/statcast-park-factors
- **Data Available**:
  - Park factors for hitting (HR, doubles, etc.) by venue
  - Batted ball data (launch angle, exit velocity impacts)
  - Detailed spray charts showing actual hit locations
  - Historical data back to 1999
  - Can filter by batter handedness, day/night, roof status
- **Data Format**: Web interface (needs scraping) or potentially available via MLB Stats API
- **Cost**: Free (public website)
- **Quality**: Official MLB Statcast data - highest quality
- **Implementation**:
  - Scrape park factors table for all 30 venues
  - Store in `venue.statcast_park_factors` table
  - Update quarterly (park factors change with each season)

#### 2. **Seamheads.com Ballparks Database** (High Priority)
- **URL**: https://www.seamheads.com/ballparks/about.php
- **Data Available**:
  - Complete ballpark history (all stadiums ever used)
  - Season-by-season field dimensions (accounts for renovations)
  - Wall heights along outfield fence
  - Fair territory area calculations
  - Backstop distances
  - 1-year park factors
- **Data Format**: Web interface (needs scraping)
- **Cost**: Free
- **Quality**: Very detailed, historically comprehensive
- **Implementation**:
  - Scrape dimension data for active MLB ballparks
  - Create historical dimension tracking (venue_id, season, dimensions)
  - Cross-reference with MLB API venue data

#### 3. **Clem's Baseball - Stadium Dimensions** (Medium Priority)
- **URL**: http://www.andrewclem.com/Baseball/Dimensions.html
- **Data Available**:
  - Nominal (marked) vs actual outfield dimensions
  - Power alley estimates
  - Detailed ballpark diagrams (visual only)
  - Historical changes documented
- **Data Format**: HTML tables (scrapable)
- **Cost**: Free
- **Quality**: Well-researched, includes power alley estimates
- **Implementation**:
  - Scrape for validation of MLB API data
  - Use power alley estimates to improve shape interpolation

#### 4. **CAD/SVG Ballpark Drawings** (Low Priority - Complex)
- **Sources**:
  - Dimensions.com (requires Pro membership - paid)
  - CADBlocksForFree.com (generic MLB field, not venue-specific)
  - GrabCAD.com (Little League fields, not MLB)
- **Data Available**:
  - DWG (CAD) format drawings
  - SVG vector graphics (can be converted from DWG)
  - Precise coordinate data for field shapes
- **Cost**: Varies (free for generic, $$ for detailed)
- **Quality**: Very high precision if available
- **Implementation Complexity**: High
  - Would need CAD file parsing library
  - Extract coordinates from vector paths
  - Convert to PostGIS geometry types
- **Recommendation**: Only pursue if we need exact shape data for simulation purposes

#### 5. **FanGraphs Community - Complete Outfield Dimensions** (Medium Priority)
- **URL**: https://community.fangraphs.com/complete-outfield-dimensions/
- **Data Available**:
  - Piecewise function equations for outfield fences
  - Uses linear functions or ellipses mapped to polar coordinates
  - Equations break whenever wall changes direction
  - Sourced from ESPN Home Run Tracker, Wikipedia, Clem's Baseball
- **Data Format**: Article with equations (manual extraction needed)
- **Cost**: Free
- **Quality**: Good mathematical model
- **Implementation**:
  - Manually extract equations for each ballpark
  - Store equations as JSONB in `venue.fence_equations` table
  - Generate fence coordinates programmatically from equations
  - Can render as SVG/GeoJSON for visualization

### Proposed Implementation Strategy

#### Phase 1: Enhance Current Data (Immediate)
1. **Augment MLB API data** with 7 outfield measurements
2. **Add Baseball Savant park factors** via web scraping
3. **Validate coordinates** against Seamheads.com
4. **Store in normalized tables**:
   ```sql
   CREATE TABLE venue.venues (
       venue_id INT PRIMARY KEY,
       -- Basic info from MLB API
       name VARCHAR(100),
       latitude NUMERIC(10, 7),
       longitude NUMERIC(10, 7),
       -- Outfield distances (7 points from MLB API)
       left_line INT,
       left_field INT,
       left_center INT,
       center_field INT,
       right_center INT,
       right_field INT,
       right_line INT,
       -- Additional metadata
       wall_height_left INT,
       wall_height_center INT,
       wall_height_right INT,
       ...
   );

   CREATE TABLE venue.statcast_park_factors (
       venue_id INT REFERENCES venue.venues(venue_id),
       season INT,
       stat_type VARCHAR(50),  -- 'HR', '2B', '3B', etc.
       park_factor NUMERIC(5, 2),  -- 100 = league average
       sample_size INT,
       updated_at TIMESTAMPTZ
   );

   CREATE TABLE venue.fence_coordinates (
       venue_id INT REFERENCES venue.venues(venue_id),
       season INT,
       angle NUMERIC(5, 2),  -- degrees from home plate (0° = center field)
       distance_feet INT,
       wall_height_feet INT,
       description TEXT  -- e.g., "Green Monster", "Pesky's Pole"
   );
   ```

#### Phase 2: Detailed Shape Data (Future Enhancement)
1. **Extract piecewise equations** from FanGraphs article
2. **Generate fence coordinates** at 1° intervals (360 points)
3. **Store as PostGIS geometry** for spatial queries
4. **Render as SVG/GeoJSON** for interactive visualizations

#### Phase 3: Historical Tracking (Future Enhancement)
1. **Scrape Seamheads.com** for historical dimension changes
2. **Track renovations** in `venue.dimension_history` table
3. **Enable time-based queries** (e.g., "What were Yankee Stadium dimensions in 2009?")

### Data Quality Considerations

**Current MLB API Data Quality:**
- ✅ Coordinates: High quality (verified via game data)
- ✅ 7 outfield distances: Good (official measurements)
- ⚠️ Wall heights: Limited (only min/max in some cases)
- ❌ Complete fence shape: Not available

**Recommended Validation Approach:**
1. Cross-reference MLB API data with Seamheads.com
2. Flag discrepancies for manual review
3. Prefer official MLB sources when conflicts arise
4. Document data provenance in `data_source` column

### Estimated Effort

- **Phase 1 (Basic Enhancement)**: 2-3 days
  - Web scraping scripts for Baseball Savant and Seamheads
  - Database schema updates
  - Data validation and cleaning

- **Phase 2 (Detailed Shapes)**: 5-7 days
  - Manual extraction of piecewise equations
  - Coordinate generation algorithms
  - PostGIS integration and testing

- **Phase 3 (Historical Tracking)**: 3-5 days
  - Historical data scraping
  - Schema versioning for dimension changes
  - Migration scripts for existing data

### Immediate Next Steps

1. ✅ **Research completed** - identified 5 primary data sources
2. **Create web scraping scripts**:
   - [ ] `scripts/scrape_baseball_savant_park_factors.py`
   - [ ] `scripts/scrape_seamheads_ballparks.py`
   - [ ] `scripts/scrape_clems_baseball.py`
3. **Enhance venue schema** with additional dimension fields
4. **Create ETL job** for periodic park factor updates
5. **Add to POC dashboard**: Ballpark comparison tool

---

**Last Updated**: 2024-11-09
**Owner**: Data Engineering Team
**Next Review**: After Phase 1 completion
