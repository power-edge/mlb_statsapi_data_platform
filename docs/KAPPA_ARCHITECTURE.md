# Kappa Architecture - Unified Batch + Streaming

**Version**: 1.0
**Last Updated**: 2024-11-15

---

## 🎯 What is Kappa Architecture?

**Kappa Architecture** is a software architecture pattern for processing streaming data. Unlike Lambda Architecture (which maintains separate batch and streaming codebases), Kappa uses **a single code path for both batch and real-time processing**.

### Lambda vs Kappa Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              LAMBDA ARCHITECTURE (Old Way)                   │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Data Source                                                │
│       ↓                                                      │
│   ┌───┴───┐                                                 │
│   │       │                                                 │
│   ↓       ↓                                                 │
│  Batch   Stream                                             │
│  Layer   Layer                                              │
│   │       │                                                 │
│   │       │  ← Two separate codebases                      │
│   │       │  ← Different logic and bugs                    │
│   │       │  ← Double maintenance burden                   │
│   ↓       ↓                                                 │
│  Batch   Stream                                             │
│  Views   Views                                              │
│   │       │                                                 │
│   └───┬───┘                                                 │
│       ↓                                                      │
│  Serving Layer                                              │
│   (Merge views)                                             │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│              KAPPA ARCHITECTURE (Our Approach)               │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Data Source (MLB Stats API)                                │
│       ↓                                                      │
│  Stream (Ingestion)                                          │
│       ↓                                                      │
│  ┌────────────────────────────────┐                         │
│  │  SINGLE TRANSFORMATION LOGIC   │                         │
│  │  (BaseTransformation)          │                         │
│  │                                │                         │
│  │  • Same code for batch/stream  │                         │
│  │  • Switch execution mode only  │                         │
│  │  • One set of tests            │                         │
│  │  • One codebase to maintain    │                         │
│  └────────────────────────────────┘                         │
│       ↓                                                      │
│  Serving Layer (PostgreSQL + Delta Lake)                    │
│       ↓                                                      │
│  Query/Analytics/Notebooks                                   │
└─────────────────────────────────────────────────────────────┘
```

### Key Principles

1. **Single Source of Truth**: All data flows through one processing pipeline
2. **Replayable Stream**: Treat historical data as a stream (replay from beginning)
3. **Unified Code**: Same transformation logic for batch backfills and real-time
4. **Immutable Events**: Source data never changes (append-only)
5. **Defensive Writes**: Timestamp-based upserts ensure correctness

---

## 🏗️ Our Kappa Implementation

### How We Achieve Kappa Architecture

```python
class BaseTransformation(ABC):
    """Single transformation logic for batch AND streaming."""

    def __init__(
        self,
        spark: SparkSession,
        endpoint: str,
        method: str,
        mode: TransformMode = TransformMode.BATCH  # ← Only difference!
    ):
        self.mode = mode
        # ... rest of init

    def read_source(self) -> DataFrame:
        """Read data - batch or streaming based on mode."""
        if self.mode == TransformMode.STREAMING:
            return self.spark.readStream.format(...).load()
        else:
            return self.spark.read.format(...).load()

    def transform(self, df: DataFrame) -> Dict[str, DataFrame]:
        """
        SAME transformation logic regardless of mode!

        This is the key to Kappa architecture:
        - Array explosion works the same
        - Field extraction works the same
        - Data quality checks work the same
        - Schema validation works the same
        """
        # ... transformation logic
        return target_dfs

    def write_targets(self, dfs: Dict[str, DataFrame]) -> Any:
        """Write data - batch or streaming based on mode."""
        if self.mode == TransformMode.STREAMING:
            return self._write_streaming(dfs)
        else:
            return self._write_batch(dfs)
```

### Batch Mode: Historical Backfills

**Use Case**: Load 5 years of historical game data

```python
# Batch processing: read all historical data at once
transform = GameLiveV1Transformation(
    spark,
    mode=TransformMode.BATCH
)

metrics = transform.run(
    start_date=date(2019, 1, 1),
    end_date=date(2024, 12, 31)
)

# Processes ALL data in one Spark job
# Uses DataFrame.write.jdbc(..., mode="append")
```

### Streaming Mode: Real-Time Games

**Use Case**: Process live games as they happen

```python
# Streaming processing: continuous processing
transform = GameLiveV1Transformation(
    spark,
    mode=TransformMode.STREAMING  # ← Only change!
)

query = transform.run(
    checkpoint_location="s3://checkpoints/game-live"
)

# Processes micro-batches every 30 seconds
# Uses DataFrame.writeStream.foreachBatch(...)
# Same transformation logic as batch!
```

---

## 🔄 Data Flow in Kappa Architecture

### Layer 1: Ingestion (Always Streaming)

```
MLB Stats API
    ↓ (every 30 seconds for live games)
    ↓ (daily batch for schedules)
game.live_game_v1 (raw JSONB table)
    ↓
    ├─ captured_at: TIMESTAMPTZ ← Timestamp is key!
    ├─ game_pk: BIGINT
    └─ data: JSONB (complete API response)
```

**Key**: Every ingestion has a timestamp. This enables:
- Replaying history (treat old data as a stream)
- Defensive upserts (never overwrite newer data)
- Time-travel queries (Delta Lake)

### Layer 2: Transformation (Batch or Stream - Same Code!)

```
Read game.live_game_v1
    ↓
Transform to 17 normalized tables
    │
    ├─ Batch Mode:
    │   • Read ALL rows matching filter
    │   • Process in parallel (Spark partitions)
    │   • Write once when complete
    │
    └─ Stream Mode:
        • Read new rows only (streaming query)
        • Process micro-batches (every 30s)
        • Write continuously
        • Checkpoint progress for fault tolerance

    ↓
game.live_game_metadata
game.live_game_plays
game.live_game_pitch_events
... (14 more tables)
```

### Layer 3: Serving (Always Up-to-Date)

```
Normalized Tables (PostgreSQL)
    ↓
    ├─ Batch reads: SELECT * FROM game.live_game_plays
    ├─ Streaming reads: Change Data Capture (CDC)
    └─ Materialized views: Aggregations

    ↓
Delta Lake (S3) - For Analytics
    ├─ Parquet files (columnar storage)
    ├─ ACID transactions
    ├─ Time travel (query old versions)
    └─ Z-ordering (optimized reads)
```

---

## 🛡️ Why Kappa Works for Our Use Case

### 1. **MLB Data is Naturally Streaming**

Games happen in real-time, but we can also replay historical games:

```python
# Real-time: Stream live games
for game in live_games:
    poll_every_30_seconds(game)

# Historical: Replay 2019 World Series Game 7
replay_game(game_pk=747175, mode=TransformMode.BATCH)
```

Both use **the same transformation code**!

### 2. **Defensive Upserts Enable Idempotency**

Because we timestamp every ingestion, we can safely re-run transformations:

```sql
MERGE INTO game.live_game_plays AS target
USING new_data AS source
ON target.game_pk = source.game_pk
   AND target.play_id = source.play_id
WHEN MATCHED AND source.captured_at >= target.captured_at THEN
    UPDATE SET *  -- Only if source is newer!
WHEN NOT MATCHED THEN
    INSERT *
```

This means:
- Re-running a job won't corrupt data
- Late-arriving data is handled correctly
- Backfills are safe

### 3. **Single Codebase = Less Maintenance**

**Without Kappa** (Lambda Architecture):
```
batch_transform.py     (500 lines)
stream_transform.py    (500 lines)
                       ─────────────
                       1000 lines total
                       2× testing effort
                       2× bug surfaces
```

**With Kappa**:
```
base_transform.py      (500 lines)
mode = BATCH | STREAM  (configuration)
                       ─────────────
                       500 lines total
                       1× testing effort
                       1× bug surface
```

### 4. **Replay from Beginning = Data Recovery**

If a transformation has a bug and produces bad data:

```python
# 1. Fix the bug in transformation code
# 2. Replay entire history (batch mode)
transform.run(
    start_date=date(2019, 1, 1),  # Start from beginning
    end_date=date.today(),         # To now
    mode=TransformMode.BATCH
)

# 3. Defensive upserts ensure:
#    - Corrected data overwrites bad data
#    - Newer data is never touched
```

No need for complex "batch layer" or "serving layer merging"!

---

## 📊 Kappa Architecture Comparison Table

| Aspect | Lambda Architecture | Kappa Architecture (Ours) |
|--------|---------------------|---------------------------|
| **Codebases** | 2 (batch + stream) | 1 (unified) |
| **Complexity** | High | Low |
| **Consistency** | Eventual (merge views) | Immediate |
| **Bug Surface** | 2× (batch bugs + stream bugs) | 1× |
| **Testing Effort** | 2× | 1× |
| **Historical Processing** | Separate batch jobs | Replay stream |
| **Real-time Processing** | Stream layer | Same code, stream mode |
| **Data Corrections** | Reprocess both layers | Replay stream |
| **Best For** | Complex analytics with different logic | Unified event processing |

---

## 🎯 When to Use Batch vs Stream Mode

### Use **Batch Mode** for:

1. **Historical Backfills**
   ```python
   # Load all 2023 season games
   transform.run(
       season="2023",
       mode=TransformMode.BATCH
   )
   ```

2. **Daily Overnight Processing**
   ```python
   # Process yesterday's completed games
   transform.run(
       date=yesterday,
       mode=TransformMode.BATCH
   )
   ```

3. **Data Corrections / Replays**
   ```python
   # Reprocess specific games after bug fix
   transform.run(
       game_pks=[747175, 747176, 747177],
       mode=TransformMode.BATCH
   )
   ```

4. **Testing / Development**
   ```bash
   # Test transformation with sample data
   uv run mlb-etl transform \
     --endpoint game \
     --method live_game_v1 \
     --mode batch \
     --game-pks 747175
   ```

### Use **Stream Mode** for:

1. **Live Game Processing**
   ```python
   # Stream live games during the season
   transform.run(
       mode=TransformMode.STREAMING,
       checkpoint_location="s3://checkpoints/live-games"
   )
   ```

2. **Real-Time Analytics**
   ```python
   # Feed dashboards with live data
   transform.run(
       mode=TransformMode.STREAMING,
       trigger="10 seconds"  # Update every 10s
   )
   ```

3. **Change Data Capture (CDC)**
   ```python
   # Stream changes from raw table to normalized tables
   transform.run(
       mode=TransformMode.STREAMING,
       read_change_stream=True
   )
   ```

---

## 🔧 Practical Example: Season 2024 Workflow

### Phase 1: Backfill Historical Data (Batch)

```python
# Argo Workflow: season-2024-backfill
for date in dates_from("2024-03-28", "2024-09-29"):
    # 1. Fetch schedule for date (batch)
    ingest_schedule(date, mode="batch")

    # 2. Get all games for that date
    games = get_games_for_date(date)

    # 3. Transform all games (batch mode)
    for game_pk in games:
        GameLiveV1Transformation(mode=BATCH).run(game_pk=game_pk)
```

**Result**: All historical 2024 regular season data loaded

### Phase 2: Stream Playoff Games (Streaming)

```python
# Argo Workflow: playoffs-2024-streaming
playoff_games = get_playoff_games("2024")

for game in playoff_games:
    # Start streaming 30 min before first pitch
    GameLiveV1Transformation(mode=STREAMING).run(
        game_pk=game.game_pk,
        start_time=game.start_time - timedelta(minutes=30),
        poll_interval=30  # Poll API every 30 seconds
    )

    # Automatically stops when game.status = "Final"
```

**Result**: Real-time playoff data with <30s latency

### Phase 3: Correctness Check (Batch Replay)

```python
# Found a bug in pitch extraction logic!
# Fix bug, then replay affected games:

GameLiveV1Transformation(mode=BATCH).run(
    start_date=date(2024, 10, 1),  # Just playoffs
    end_date=date(2024, 11, 1),
    force_refresh=True
)

# Defensive upserts ensure:
# - Corrected data replaces buggy data
# - Live streaming continues unaffected
```

---

## 🎓 Benefits of Our Kappa Implementation

### 1. **Developer Experience**
- Write transformation logic **once**
- Test **once**
- Deploy **once**
- Maintain **once**

### 2. **Operational Simplicity**
- No view merging
- No complex orchestration between batch and stream
- Single monitoring dashboard
- One set of metrics

### 3. **Data Quality**
- Consistent transformations (no drift between batch and stream)
- Defensive upserts prevent data corruption
- Easy to replay and verify

### 4. **Cost Efficiency**
- Less code = less compute
- Batch mode for bulk (cheaper)
- Stream mode for real-time (as needed)
- No duplicate processing

### 5. **Scalability**
- Spark handles both batch and streaming
- Same performance optimizations apply
- Easy to add resources (more executors)

---

## 🚀 Future Phases: Query & Analytics (Not In Scope Yet)

While we're focused on the **data mart and pipelines** now, here's what comes later:

### Phase N+1: Query Layer
- **Presto/Trino**: Federated SQL across PostgreSQL + S3
- **Apache Superset**: Self-service BI dashboards
- **GraphQL API**: Flexible querying for applications

### Phase N+2: Notebook Environment
- **JupyterLab**: Ad-hoc analysis
- **Zeppelin**: Collaborative notebooks
- **Integration with PySpark transformations**

### Phase N+3: Visualization & Reporting
- **Grafana**: Operational dashboards
- **Tableau/PowerBI**: Executive reporting (via ODBC)
- **Custom React dashboards**: Powered by GraphQL

**Right now**: We focus on getting **high-quality, well-modeled data** into the data mart. Everything else builds on that foundation!

---

## 📚 Further Reading

- [Original Kappa Architecture Blog Post](http://milinda.pathirage.org/kappa-architecture.com/)
- [Questioning the Lambda Architecture](https://www.oreilly.com/radar/questioning-the-lambda-architecture/)
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake: Unifying Batch and Streaming](https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html)

---

**Key Takeaway**: Kappa Architecture means **one transformation codebase, two execution modes**. This dramatically simplifies our data platform while maintaining flexibility for both historical backfills and real-time streaming.
