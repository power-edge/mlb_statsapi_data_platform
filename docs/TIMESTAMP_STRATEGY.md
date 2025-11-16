# Timestamp Strategy for Defensive Upserts

**Date**: 2024-11-15
**Status**: ✅ Defined

## Overview

We use **two timestamps** to track data provenance and enable defensive upserts:

1. **`captured_at`** - When the API request was executed (source of truth)
2. **`ingested_at`** - When the data was written to our database (audit)

## Timestamp Definitions

### `captured_at` (Primary Timestamp)

**Definition**: The exact moment when the API request was sent and the response was received.

**Purpose**:
- **Defensive upserts** - Determines data freshness
- **Data versioning** - Identifies which snapshot is newer
- **Time-travel** - Reconstruct state at any point in time
- **SLA tracking** - Measure API latency

**Source**: Set by the API client at the moment of request execution

**Format**: ISO 8601 with timezone (UTC), microsecond precision
```
2024-10-25T14:30:05.123456Z
```

**Critical Properties**:
- ✅ Monotonically increasing (for same resource)
- ✅ Reflects actual game state time
- ✅ Independent of ingestion delays
- ✅ Can be compared across workers

### `ingested_at` (Secondary Timestamp)

**Definition**: The moment when data was written to the database.

**Purpose**:
- **Audit trail** - When did we store this data?
- **Lag detection** - How long from capture to storage?
- **Partition key** - For time-based partitioning
- **Pipeline SLA** - Measure ingestion latency

**Source**: Set by database during INSERT (DEFAULT CURRENT_TIMESTAMP)

**Format**: PostgreSQL TIMESTAMPTZ (timezone-aware)

**Critical Properties**:
- ✅ Reflects storage time, not data time
- ✅ Always present (database default)
- ✅ Never null
- ✅ Independent of API request time

## Why Both Timestamps Matter

### Scenario: Race Condition

```
Timeline:
14:00:00 - Worker 1 fetches game (inning 7)
14:00:05 - Worker 1 starts ingestion
14:00:10 - Worker 2 fetches game (inning 9, game finished)
14:00:12 - Worker 2 starts ingestion
14:00:15 - Worker 2 finishes writing to DB ✅
14:00:20 - Worker 1 finishes writing to DB (LATE!)

Without captured_at:
  - Worker 1's data would overwrite Worker 2's data (inning 9 → inning 7)
  - Database would have STALE data ❌

With captured_at:
  - Worker 1: captured_at=14:00:00, ingested_at=14:00:20
  - Worker 2: captured_at=14:00:10, ingested_at=14:00:15
  - Defensive upsert: Worker 1's captured_at < Worker 2's captured_at → SKIP
  - Database preserves Worker 2's NEWER data ✅
```

### Scenario: Backfill vs Live Data

```
Current time: 15:00:00

Database has:
  game_pk=747175, captured_at=14:00:00, ingested_at=14:00:05 (live fetch)

Backfill job runs (replaying historical data):
  game_pk=747175, captured_at=10:00:00, ingested_at=15:00:00 (NOW)

Without captured_at:
  - ingested_at comparison: 15:00:00 > 14:00:05 → UPDATE
  - Live data gets overwritten by old data ❌

With captured_at:
  - captured_at comparison: 10:00:00 < 14:00:00 → SKIP
  - Live data preserved ✅
```

## Raw Table Schema

```sql
CREATE TABLE game.live_game_v1 (
    -- Identity
    id BIGSERIAL PRIMARY KEY,
    game_pk BIGINT NOT NULL,

    -- Timestamps (CRITICAL!)
    captured_at TIMESTAMPTZ NOT NULL,     -- ⭐ Used for defensive upserts
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- API Request Metadata
    endpoint VARCHAR(50) NOT NULL,
    method VARCHAR(50) NOT NULL,
    path_params JSONB NOT NULL,
    query_params JSONB NOT NULL,
    request_url TEXT NOT NULL,
    http_status_code INT NOT NULL,

    -- Schema Version
    schema_version VARCHAR(10) NOT NULL,

    -- Response Data
    data JSONB NOT NULL,

    -- Indexes
    INDEX idx_game_pk (game_pk),
    INDEX idx_captured_at (captured_at),
    INDEX idx_ingested_at (ingested_at),
    INDEX idx_endpoint_method (endpoint, method),

    -- Constraints
    CONSTRAINT chk_captured_at_valid CHECK (captured_at <= ingested_at),
    CONSTRAINT chk_status_code_valid CHECK (http_status_code BETWEEN 100 AND 599)

) PARTITION BY RANGE (captured_at);  -- Partition by capture time, not ingest time!
```

## Defensive Upsert SQL

```sql
INSERT INTO game.live_game_v1 (
    game_pk,
    captured_at,      -- ⭐ Source timestamp
    ingested_at,      -- Auto-populated by DEFAULT
    endpoint,
    method,
    path_params,
    query_params,
    request_url,
    http_status_code,
    schema_version,
    data
)
SELECT
    game_pk,
    captured_at,      -- From source
    CURRENT_TIMESTAMP,  -- Ingestion time
    endpoint,
    method,
    path_params,
    query_params,
    request_url,
    http_status_code,
    schema_version,
    data
FROM temp_table_source
ON CONFLICT (game_pk)
DO UPDATE SET
    captured_at = EXCLUDED.captured_at,
    ingested_at = CURRENT_TIMESTAMP,  -- Update ingest time
    endpoint = EXCLUDED.endpoint,
    method = EXCLUDED.method,
    path_params = EXCLUDED.path_params,
    query_params = EXCLUDED.query_params,
    request_url = EXCLUDED.request_url,
    http_status_code = EXCLUDED.http_status_code,
    schema_version = EXCLUDED.schema_version,
    data = EXCLUDED.data
WHERE EXCLUDED.captured_at >= game.live_game_v1.captured_at;  -- ⭐ DEFENSIVE!
```

## Implementation: Capture Timestamp

### In pymlb_statsapi (API Client)

```python
from datetime import datetime, timezone
import httpx

def fetch_game_data(game_pk: int) -> dict:
    """Fetch game data with captured_at timestamp."""

    # Record request time BEFORE making API call
    captured_at = datetime.now(timezone.utc)

    # Make API request
    response = httpx.get(
        f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
    )

    # Build metadata wrapper
    return {
        "endpoint": "game",
        "method": "liveGameV1",
        "path_params": {"game_pk": str(game_pk)},
        "query_params": {},
        "url": str(response.url),
        "status_code": response.status_code,
        "captured_at": captured_at.isoformat(),  # ⭐ ISO 8601 string
        "response": response.json(),
    }
```

### In Ingestion Job

```python
from datetime import datetime, timezone
import json

def ingest_game(game_pk: int):
    """Ingest game data with both timestamps."""

    # Fetch data (includes captured_at from API client)
    api_result = mlb.game.liveGameV1(game_pk=game_pk)

    # Parse captured_at from API result
    captured_at = datetime.fromisoformat(api_result["captured_at"])

    # Prepare row for database
    row = {
        "game_pk": game_pk,
        "captured_at": captured_at,  # From API client
        "ingested_at": datetime.now(timezone.utc),  # Now
        "endpoint": api_result["endpoint"],
        "method": api_result["method"],
        "path_params": json.dumps(api_result["path_params"]),
        "query_params": json.dumps(api_result["query_params"]),
        "request_url": api_result["url"],
        "http_status_code": api_result["status_code"],
        "schema_version": "1.1",
        "data": json.dumps(api_result["response"]),
    }

    # Write to database (uses captured_at for defensive upsert)
    write_to_raw_table(row)
```

### In PySpark Transformation

```python
from pyspark.sql import functions as F

# Read raw data
source_df = spark.read.jdbc(url, "game.live_game_v1")

# Filter by captured_at (not ingested_at!)
recent_games = source_df.filter(
    F.col("captured_at") >= "2024-10-25"  # Game data from Oct 25+
)

# Transform with defensive upsert
defensive_upsert_postgres(
    spark=spark,
    source_df=recent_games,
    target_table="game.live_game_metadata",
    primary_keys=["game_pk"],
    timestamp_column="captured_at",  # ⭐ Use captured_at for comparison!
)
```

## Monitoring & Alerts

### Lag Detection

```sql
-- Measure ingestion lag
SELECT
    game_pk,
    captured_at,
    ingested_at,
    EXTRACT(EPOCH FROM (ingested_at - captured_at)) as lag_seconds
FROM game.live_game_v1
WHERE captured_at >= NOW() - INTERVAL '1 hour'
ORDER BY lag_seconds DESC
LIMIT 20;

-- Alert if lag > 5 minutes
SELECT COUNT(*) as games_with_high_lag
FROM game.live_game_v1
WHERE captured_at >= NOW() - INTERVAL '1 hour'
  AND (ingested_at - captured_at) > INTERVAL '5 minutes';
```

### Out-of-Order Detection

```sql
-- Detect writes where captured_at is older than existing data
SELECT
    game_pk,
    LAG(captured_at) OVER (PARTITION BY game_pk ORDER BY ingested_at) as prev_captured_at,
    captured_at as current_captured_at,
    ingested_at,
    CASE
        WHEN captured_at < LAG(captured_at) OVER (PARTITION BY game_pk ORDER BY ingested_at)
        THEN 'OUT_OF_ORDER'
        ELSE 'IN_ORDER'
    END as order_status
FROM game.live_game_v1
WHERE ingested_at >= NOW() - INTERVAL '1 hour'
ORDER BY game_pk, ingested_at;
```

### Defensive Upsert Effectiveness

```sql
-- Show which writes were skipped by defensive upserts
-- (requires tracking in separate audit table)
SELECT
    game_pk,
    source_captured_at,
    target_captured_at,
    'SKIPPED' as action,
    target_captured_at - source_captured_at as time_diff
FROM upsert_audit_log
WHERE action = 'skipped'
  AND timestamp >= NOW() - INTERVAL '1 day'
ORDER BY time_diff DESC;
```

## Best Practices

1. **Always set `captured_at` at API request time** - Not after, not before
2. **Use UTC timezone** - Avoid timezone ambiguity
3. **Microsecond precision** - Distinguish rapid successive requests
4. **Validate constraint** - `captured_at <= ingested_at` (can't ingest before capture)
5. **Partition by `captured_at`** - Aligns with data's natural timeline
6. **Index both timestamps** - Efficient querying on either dimension
7. **Monitor lag** - Alert if `ingested_at - captured_at > threshold`
8. **Never modify `captured_at`** - It represents the data's true time

## Common Pitfalls to Avoid

❌ **Don't use `ingested_at` for defensive upserts**
```python
# WRONG - compares write times, not data times
WHERE EXCLUDED.ingested_at >= target.ingested_at  # ❌
```

✅ **Use `captured_at` for defensive upserts**
```python
# CORRECT - compares data freshness times
WHERE EXCLUDED.captured_at >= target.captured_at  # ✅
```

❌ **Don't set `captured_at` from current time in ingestion**
```python
# WRONG - loses true capture time
row["captured_at"] = datetime.now(timezone.utc)  # ❌
```

✅ **Preserve `captured_at` from API client**
```python
# CORRECT - preserves API request time
row["captured_at"] = api_result["captured_at"]  # ✅
```

❌ **Don't partition by `ingested_at`**
```sql
-- WRONG - partitions by storage time, not data time
PARTITION BY RANGE (ingested_at);  -- ❌
```

✅ **Partition by `captured_at`**
```sql
-- CORRECT - partitions by data time
PARTITION BY RANGE (captured_at);  -- ✅
```

## Summary

| Aspect | `captured_at` | `ingested_at` |
|--------|--------------|---------------|
| **Set by** | API client | Database default |
| **Represents** | Data freshness | Storage time |
| **Used for** | Defensive upserts | Audit trail |
| **Partition by** | ✅ Yes | ❌ No |
| **Compare in MERGE** | ✅ Yes | ❌ No |
| **Nullable** | ❌ Never | ❌ Never |
| **Monotonic per resource** | ✅ Yes | ⚠️ Maybe |

**Key Principle**: `captured_at` is the source of truth for data freshness. Always use it for defensive upserts, never `ingested_at`.

---

**Two timestamps, one truth: `captured_at` wins** ⏱️✅
