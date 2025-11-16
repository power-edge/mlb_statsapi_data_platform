# API Metadata Ingestion

**Status**: 📋 Documented
**Priority**: High
**Date**: 2025-11-15

## Overview

When ingesting data from the MLB Stats API, we must preserve **request metadata** alongside the response data. This metadata is crucial for:
- **Reproducibility** - Ability to replay exact API requests
- **Debugging** - Understanding what was requested vs what was returned
- **Auditing** - Tracking API usage patterns
- **Testing** - Creating accurate stub data
- **Data lineage** - Knowing the source of every piece of data

## Required Metadata Fields

Based on the stub data structure from `pymlb_statsapi`, every API ingestion must capture:

```json
{
  "endpoint": "game",
  "method": "liveGameV1",
  "path_params": {"game_pk": "747175"},
  "query_params": {},
  "url": "https://statsapi.mlb.com/api/v1.1/game/747175/feed/live",
  "status_code": 200,
  "captured_at": "2024-10-25T14:30:05.123456Z",  // ⭐ CRITICAL FOR DEFENSIVE UPSERTS!
  "response": { ... }
}
```

### Field Descriptions

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `endpoint` | string | Yes | MLB Stats API endpoint name | `"game"`, `"schedule"`, `"person"` |
| `method` | string | Yes | API method called on endpoint | `"liveGameV1"`, `"schedule"` |
| `path_params` | object | Yes | Path parameters passed to API | `{"game_pk": "747175"}` |
| `query_params` | object | Yes | Query string parameters | `{"sportId": 1, "date": "2024-10-25"}` |
| `url` | string | Yes | Complete URL that was fetched | Full URL including host and params |
| `status_code` | integer | Yes | HTTP status code from response | `200`, `404`, `500`, etc. |
| **`captured_at`** | **timestamp** | **Yes** | **⭐ When API request was made (UTC)** | **`"2024-10-25T14:30:05.123456Z"`** |
| `response` | object | Yes | Actual API response body | Game data, schedule data, etc. |

### ⭐ Why `captured_at` is Critical

The `captured_at` timestamp is **the source of truth for defensive upserts**. It represents when the API request was executed, not when data was stored. This enables:

- **Defensive upserts** - Compare data freshness, not write order
- **Race condition safety** - Slower writes don't overwrite faster writes
- **Backfill protection** - Historical replays don't corrupt live data
- **Time-travel queries** - Reconstruct game state at any point in time

See [TIMESTAMP_STRATEGY.md](./TIMESTAMP_STRATEGY.md) for complete details.

## Raw Table Schema

The raw PostgreSQL tables must include these metadata columns:

```sql
CREATE TABLE game.live_game_v1 (
    -- Primary keys
    id BIGSERIAL PRIMARY KEY,
    game_pk BIGINT NOT NULL,

    -- Temporal tracking
    captured_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Schema version
    schema_version VARCHAR(10) NOT NULL,  -- e.g., "1.1"

    -- API Request Metadata (NEW!)
    endpoint VARCHAR(50) NOT NULL,        -- "game"
    method VARCHAR(50) NOT NULL,          -- "liveGameV1"
    path_params JSONB NOT NULL,           -- {"game_pk": "747175"}
    query_params JSONB NOT NULL,          -- {}
    request_url TEXT NOT NULL,            -- Full URL
    http_status_code INT NOT NULL,        -- 200

    -- Response data
    data JSONB NOT NULL,                  -- Actual response

    -- Indexes
    INDEX idx_game_pk (game_pk),
    INDEX idx_captured_at (captured_at),
    INDEX idx_endpoint_method (endpoint, method)

) PARTITION BY RANGE (captured_at);
```

## Why Each Field Matters

### `endpoint` + `method`
- **Routing**: Determines which transformation job to use
- **Schema validation**: Each endpoint/method has different schema
- **Organization**: Natural hierarchy for data organization

**Example use case**: "Show me all requests to `game.liveGameV1` that failed"

### `path_params`
- **Resource identification**: Path parameters identify specific resources
- **Filtering**: Enable efficient lookups (e.g., all requests for `game_pk=747175`)
- **Re-fetching**: Can reconstruct exact API call

**Example use case**: "Re-fetch all games from game_pk 747000-748000"

### `query_params`
- **Filter recreation**: Query params filter data server-side
- **Cache key**: Part of cache key for responses
- **Partial updates**: Know what subset was requested

**Example use case**: "Show me all schedule requests for sportId=1 (MLB)"

### `url`
- **Complete reproducibility**: Exact URL that was hit
- **Debugging**: Compare expected vs actual URLs
- **External integration**: Can share URLs for troubleshooting

**Example use case**: "Paste this URL into browser to see raw API response"

### `status_code`
- **Error tracking**: Identify failed requests
- **Retry logic**: Different strategies for different status codes
- **Data quality**: Flag potentially incomplete data

**Example use case**: "How many 404s did we get for game_pk 747175?"

## Ingestion Implementation

### Using pymlb_statsapi

The `pymlb_statsapi` library already returns this metadata when configured with `capture_metadata=True`:

```python
from pymlb_statsapi import MLB

# Create client with metadata capture
mlb = MLB(capture_metadata=True)

# Make API request
result = mlb.game.liveGameV1(game_pk=747175)

# Result includes metadata
print(result.keys())
# dict_keys(['endpoint', 'method', 'path_params', 'query_params', 'url', 'status_code', 'response'])

# Save to database
save_to_raw_table(
    endpoint=result["endpoint"],
    method=result["method"],
    path_params=result["path_params"],
    query_params=result["query_params"],
    url=result["url"],
    status_code=result["status_code"],
    data=result["response"],
    captured_at=datetime.now(timezone.utc),
)
```

### Ingestion Job Configuration

Job configs should specify which metadata to preserve:

```yaml
# config/jobs/game_live.yaml
name: game_live_ingestion
source:
  endpoint: game
  method: liveGameV1
  capture_metadata: true  # REQUIRED!

storage:
  raw:
    table: game.live_game_v1
    preserve_metadata:  # Metadata columns to populate
      - endpoint
      - method
      - path_params
      - query_params
      - request_url
      - http_status_code
```

### Transformation Access to Metadata

Transformations can filter and use metadata:

```python
# Read raw table with metadata
source_df = spark.read.jdbc(
    url="jdbc:postgresql://localhost:5432/mlb_games",
    table="game.live_game_v1"
)

# Filter by endpoint/method
filtered_df = source_df.filter(
    (F.col("endpoint") == "game") &
    (F.col("method") == "liveGameV1") &
    (F.col("http_status_code") == 200)  # Only successful requests
)

# Extract path parameters for filtering
game_pk_df = filtered_df.withColumn(
    "game_pk_from_params",
    F.get_json_object("path_params", "$.game_pk").cast("bigint")
)
```

## MinIO/S3 Storage

When storing raw data in MinIO/S3, metadata should be in the object path:

```
s3://mlb-raw-data/
├── endpoint=game/
│   ├── method=liveGameV1/
│   │   ├── date=2024-10-25/
│   │   │   ├── game_pk=747175/
│   │   │   │   └── captured_at=20241025_143000.json.gz
│   │   │   └── game_pk=747176/
│   │   │       └── captured_at=20241025_183000.json.gz
│   │   └── date=2024-10-26/
│   │       └── ...
│   └── method=boxscore/
│       └── ...
└── endpoint=schedule/
    └── ...
```

Each JSON file contains:
```json
{
  "endpoint": "game",
  "method": "liveGameV1",
  "path_params": {"game_pk": "747175"},
  "query_params": {},
  "url": "https://statsapi.mlb.com/api/v1.1/game/747175/feed/live",
  "status_code": 200,
  "captured_at": "2024-10-25T14:30:00Z",
  "response": { ... }
}
```

## Data Quality Checks

Metadata enables powerful data quality checks:

```python
from pydeequ.checks import Check, CheckLevel

# Quality check on raw table
check = Check(spark, CheckLevel.Error, "API Metadata Quality")

check = (
    check
    .hasSize(lambda sz: sz > 0)  # Has data
    .isComplete("endpoint")      # All rows have endpoint
    .isComplete("method")        # All rows have method
    .isComplete("path_params")   # All rows have path params
    .isComplete("url")           # All rows have URL
    .isContainedIn("http_status_code", [200, 201, 304])  # Only success codes
    .isContainedIn("endpoint", ["game", "schedule", "person", "team"])  # Valid endpoints
)
```

## Audit Queries

Metadata enables comprehensive auditing:

```sql
-- 1. API usage by endpoint/method
SELECT
    endpoint,
    method,
    COUNT(*) as request_count,
    AVG(CASE WHEN http_status_code = 200 THEN 1 ELSE 0 END) as success_rate
FROM game.live_game_v1
WHERE captured_at >= NOW() - INTERVAL '1 day'
GROUP BY endpoint, method
ORDER BY request_count DESC;

-- 2. Failed requests with full context
SELECT
    endpoint,
    method,
    path_params,
    query_params,
    request_url,
    http_status_code,
    captured_at
FROM game.live_game_v1
WHERE http_status_code != 200
ORDER BY captured_at DESC
LIMIT 100;

-- 3. Most frequently requested resources
SELECT
    path_params->>'game_pk' as game_pk,
    COUNT(*) as fetch_count,
    MIN(captured_at) as first_fetched,
    MAX(captured_at) as last_fetched
FROM game.live_game_v1
WHERE endpoint = 'game' AND method = 'liveGameV1'
GROUP BY path_params->>'game_pk'
ORDER BY fetch_count DESC
LIMIT 20;

-- 4. Duplicate requests (same params, different timestamps)
SELECT
    endpoint,
    method,
    path_params,
    query_params,
    COUNT(*) as duplicate_count,
    ARRAY_AGG(captured_at ORDER BY captured_at) as fetch_timestamps
FROM game.live_game_v1
WHERE captured_at >= NOW() - INTERVAL '1 hour'
GROUP BY endpoint, method, path_params, query_params
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
```

## Replay & Debugging

Metadata enables exact request replay:

```python
def replay_request(row):
    """Replay an API request from metadata."""
    from pymlb_statsapi import MLB

    mlb = MLB()
    endpoint_obj = getattr(mlb, row["endpoint"])
    method_func = getattr(endpoint_obj, row["method"])

    # Reconstruct request
    path_params = json.loads(row["path_params"])
    query_params = json.loads(row["query_params"])

    # Execute
    result = method_func(**path_params, **query_params)

    # Compare with original
    assert result == json.loads(row["data"])

    return result

# Replay specific failed request
failed_request = spark.sql("""
    SELECT * FROM game.live_game_v1
    WHERE http_status_code = 500
    ORDER BY captured_at DESC
    LIMIT 1
""").first()

replay_request(failed_request)
```

## Migration Path

### Phase 1: Add Metadata Columns (Current)
- Add new columns to existing raw tables
- Backfill with NULL for historical data
- Make required for new ingestions

### Phase 2: Update Ingestion Jobs
- Update all job configs to capture metadata
- Deploy updated ingestion workers
- Validate metadata is populated

### Phase 3: Transformation Updates
- Update transformations to filter by `http_status_code = 200`
- Add metadata to lineage tracking
- Enable audit queries

### Phase 4: Enforce Constraints
- Add NOT NULL constraints on metadata columns
- Add CHECK constraints (e.g., `http_status_code > 0`)
- Drop old data without metadata

## Best Practices

1. **Always capture metadata** - Never save API responses without metadata
2. **Validate on ingestion** - Check that all metadata fields are populated
3. **Index metadata columns** - Enable efficient filtering
4. **Partition by date** - Use `captured_at` for time-based partitioning
5. **Preserve exact URLs** - Don't reconstruct, save the actual URL hit
6. **Log status codes** - Even successful requests should log `200`
7. **Use JSONB for params** - Enables efficient querying of nested structures

## See Also

- `tests/integration/test_e2e_transformation.py` - Shows stub data structure
- `src/mlb_data_platform/ingestion/client.py` - API client implementation
- `config/schemas/mappings/` - Schema mappings with metadata fields
- [pymlb_statsapi documentation](https://pymlb-statsapi.readthedocs.io/) - Metadata capture

---

**Every byte of metadata is a byte of reproducibility** ✅
