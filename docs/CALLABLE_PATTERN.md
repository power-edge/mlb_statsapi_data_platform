# Callable Transformation Pattern

**Why we use `__call__()` instead of `run()`**

---

## 🎯 The Problem with `.run()`

Traditional approach:
```python
class Transform:
    def run(self, **kwargs):
        # Do transformation
        pass

# Usage - feels like a method call
transform = Transform(spark)
transform.run(game_pks=[747175])
```

**Issues**:
- Not Pythonic (functions are first-class citizens in Python)
- Less intuitive (`.run()` is generic and doesn't convey intent)
- Can't be used directly in functional patterns
- Extra mental overhead (instantiate, then call `.run()`)

---

## ✅ The `__call__()` Pattern

**Pythonic approach**:
```python
class Transform:
    def __call__(self, **kwargs):
        # Do transformation
        pass

# Usage - transformation instance IS callable!
transform = Transform(spark)
metrics = transform(game_pks=[747175])  # Clean!
```

---

## 🚀 Benefits of Callable Transformations

### 1. **Intuitive API**

Transformations behave like functions:
```python
# Configure once
transform = GameLiveV1Transformation(
    spark,
    mode=TransformMode.BATCH,
    enable_quality_checks=True
)

# Call multiple times with different parameters
transform(game_pks=[747175])
transform(game_pks=[747176])
transform(season="2024")
```

### 2. **Functional Patterns**

Can be used in map/filter/reduce patterns:
```python
# List of game PKs
game_pks = [747175, 747176, 747177, 747178, 747179]

# Create transformation
transform = GameLiveV1Transformation(spark, mode=BATCH)

# Process in parallel using map
from multiprocessing import Pool

with Pool(5) as pool:
    results = pool.map(
        lambda pk: transform(game_pks=[pk]),
        game_pks
    )
```

### 3. **Cleaner Argo Workflow Integration**

```yaml
# Argo Workflow template
- name: transform-game
  script:
    image: mlb-data-platform-spark:latest
    command: [python]
    source: |
      from mlb_data_platform.transform.game import GameLiveV1Transformation

      transform = GameLiveV1Transformation(spark, mode=BATCH)

      # Call directly with parameters from workflow
      metrics = transform(
          game_pks={{inputs.parameters.game_pks}},
          validate={{inputs.parameters.validate}},
          export_to_s3={{inputs.parameters.export_to_s3}}
      )

      print(f"METRICS: {metrics}")
```

### 4. **Parameter Flexibility**

Each call can have completely different parameters:
```python
transform = GameLiveV1Transformation(spark, mode=BATCH)

# Call 1: Specific games with validation
transform(game_pks=[747175], validate=True)

# Call 2: Date range without validation (faster)
transform(start_date=date(2024, 10, 1), validate=False)

# Call 3: Partial tables only
transform(
    game_pks=[747176],
    target_tables=["game.live_game_metadata"]
)

# Call 4: With S3 export
transform(season="2024", export_to_s3=True, s3_bucket="my-bucket")
```

### 5. **Configuration Separation**

**Instance creation** = **configuration** (what doesn't change)
**Calling** = **parameters** (what changes per run)

```python
# Configuration (expensive, do once)
transform = GameLiveV1Transformation(
    spark,
    mode=TransformMode.BATCH,
    enable_quality_checks=True,      # ← Configuration
    fail_on_quality_error=True,      # ← Configuration
)

# Parameters (cheap, do many times)
transform(game_pks=[747175])         # ← Runtime parameters
transform(game_pks=[747176])         # ← Runtime parameters
transform(season="2024")             # ← Runtime parameters
```

### 6. **Testing is Cleaner**

```python
def test_game_transformation():
    """Test transformation with specific parameters."""
    transform = GameLiveV1Transformation(spark, mode=BATCH)

    # Call is just a function call - easy to test!
    metrics = transform(
        game_pks=[747175],
        validate=False,  # Skip validation in tests
        export_to_s3=False
    )

    assert metrics["source_row_count"] == 1
    assert len(metrics["target_tables"]) == 17
```

### 7. **Composition and Higher-Order Functions**

Can create transformation factories:
```python
def create_transform_pipeline(spark, mode, quality_level):
    """Factory function that returns a configured transformation."""
    return GameLiveV1Transformation(
        spark,
        mode=mode,
        enable_quality_checks=(quality_level != "none"),
        fail_on_quality_error=(quality_level == "strict")
    )

# Create different pipelines
dev_transform = create_transform_pipeline(spark, BATCH, "none")
prod_transform = create_transform_pipeline(spark, BATCH, "strict")

# Both are callable with same interface
dev_transform(game_pks=[747175])
prod_transform(game_pks=[747175])
```

### 8. **Workflow Orchestration**

Perfect for workflow engines:
```python
# Argo Workflow: Process games in parallel
from argo.workflows import Workflow

def process_season_in_parallel(season: str):
    """Process entire season using parallel transformations."""
    # Get all games for season
    games = get_games_for_season(season)

    # Create transformation (configuration)
    transform = GameLiveV1Transformation(spark, mode=BATCH)

    # Submit parallel jobs to Argo
    workflows = []
    for game_pk in games:
        wf = Workflow.submit(
            lambda: transform(game_pks=[game_pk])  # ← Callable!
        )
        workflows.append(wf)

    # Wait for all to complete
    return [wf.result() for wf in workflows]
```

---

## 📊 Comparison: `run()` vs `__call__()`

| Aspect | `.run()` Pattern | `__call__()` Pattern |
|--------|-----------------|---------------------|
| **Pythonic** | ❌ Method call | ✅ Function-like |
| **Intent** | Generic `.run()` | Clear (instance IS the action) |
| **Functional Patterns** | ❌ Can't use in map/filter | ✅ Works with map/filter/reduce |
| **Readability** | `transform.run(x)` | `transform(x)` |
| **Reusability** | ❌ Need to wrap | ✅ Already callable |
| **Testing** | Extra boilerplate | Clean function calls |
| **Workflow Integration** | More verbose | Cleaner syntax |

---

## 🎓 Real-World Example: Season Backfill

**With `.run()` (old way)**:
```python
transform = GameLiveV1Transformation(spark, mode=BATCH)

for date in season_dates:
    games = get_games_for_date(date)
    for game_pk in games:
        transform.run(game_pks=[game_pk])  # ← Extra `.run()`
```

**With `__call__()` (new way)**:
```python
transform = GameLiveV1Transformation(spark, mode=BATCH)

for date in season_dates:
    games = get_games_for_date(date)
    for game_pk in games:
        transform(game_pks=[game_pk])  # ← Direct call!
```

Even better with functional patterns:
```python
transform = GameLiveV1Transformation(spark, mode=BATCH)

# Get all games, process in parallel
all_games = [game_pk for date in season_dates for game_pk in get_games_for_date(date)]

with ThreadPoolExecutor(max_workers=10) as executor:
    metrics = executor.map(
        lambda pk: transform(game_pks=[pk]),  # ← Callable!
        all_games
    )
```

---

## 🏗️ How It Works Under the Hood

```python
class GameLiveV1Transformation(BaseTransformation):
    """Transformation that processes game data."""

    def __init__(self, spark, mode, **config):
        """Initialize with configuration (expensive)."""
        super().__init__(spark, "game", "live_game_v1", mode)
        self.config = config
        # Load schemas, setup connections, etc.

    def __call__(self, **params):
        """Execute transformation with runtime parameters (cheap).

        This is the workflow orchestrator:
        1. Read source data (filtered by params)
        2. Transform to targets
        3. Validate data quality
        4. Write to PostgreSQL
        5. Export to S3/Delta
        6. Return metrics
        """
        # Step 1: Read
        df = self.read_source(
            filter_condition=self._build_filter(**params)
        )

        # Step 2: Transform
        targets = self.transform(df, params.get('target_tables'))

        # Step 3: Validate
        if params.get('validate', True):
            self._validate(targets)

        # Step 4: Write
        metrics = self.write_targets(targets)

        # Step 5: Export
        if params.get('export_to_s3'):
            self._export_s3(targets, params['s3_bucket'])

        return metrics
```

The `__call__` method **orchestrates the workflow** around the core transformation logic inherited from `BaseTransformation`.

---

## 🎯 Key Takeaway

**`__call__()` separates configuration from execution**:

- **Configuration** (instance creation) = what stays the same
- **Execution** (calling) = what changes per run

This makes transformations:
- More reusable
- Easier to test
- Cleaner to compose
- More Pythonic
- Better for workflows

---

**Bottom Line**: `transform(params)` is more Pythonic and flexible than `transform.run(params)` ✅
