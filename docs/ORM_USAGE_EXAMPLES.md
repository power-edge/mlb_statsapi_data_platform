# ORM Usage Examples

**Auto-generated SQLModel classes for type-safe database access**

## Overview

All database tables have corresponding SQLModel ORM classes that provide:
- **Type safety** - Pydantic validation and type hints
- **IDE autocomplete** - Full IntelliSense support
- **Query builders** - Pythonic query syntax instead of raw SQL
- **Relationships** - Navigate between related tables
- **Validation** - Automatic data validation on insert/update

## Installation

SQLModel is already included in the project dependencies:

```bash
uv sync
```

## Basic Usage

### Creating Instances

```python
from datetime import date
from mlb_data_platform.models import LiveGameMetadata

# Create a game metadata instance
game = LiveGameMetadata(
    game_pk=747175,
    game_date=date(2024, 10, 25),
    game_type="W",  # World Series
    home_team_name="Los Angeles Dodgers",
    away_team_name="New York Yankees",
    home_score=7,
    away_score=6,
    abstract_game_state="Final",
)

# Pydantic validation happens automatically
print(game.game_pk)  # 747175
print(game.home_team_name)  # "Los Angeles Dodgers"
```

### Database Connection

```python
from sqlmodel import create_engine, Session

# Create engine
engine = create_engine("postgresql://mlb_admin:password@localhost:5432/mlb_games")

# Use session for queries
with Session(engine) as session:
    # Your queries here
    pass
```

### Inserting Data

```python
from sqlmodel import Session
from mlb_data_platform.models import LiveGameMetadata

with Session(engine) as session:
    game = LiveGameMetadata(
        game_pk=747175,
        game_date=date(2024, 10, 25),
        home_team_name="Los Angeles Dodgers",
        away_team_name="New York Yankees",
    )

    session.add(game)
    session.commit()
    session.refresh(game)  # Refresh to get any DB-generated values
```

### Querying Data

#### Simple Select

```python
from sqlmodel import Session, select
from mlb_data_platform.models import LiveGameMetadata

with Session(engine) as session:
    # Get single game by primary key
    statement = select(LiveGameMetadata).where(LiveGameMetadata.game_pk == 747175)
    game = session.exec(statement).first()

    if game:
        print(f"{game.away_team_name} @ {game.home_team_name}")
        print(f"Score: {game.away_score} - {game.home_score}")
```

#### Filtering

```python
from sqlmodel import Session, select
from mlb_data_platform.models import LiveGameMetadata

with Session(engine) as session:
    # Get all World Series games
    statement = select(LiveGameMetadata).where(
        LiveGameMetadata.game_type == "W"
    )

    world_series_games = session.exec(statement).all()

    for game in world_series_games:
        print(f"Game {game.game_pk}: {game.away_team_name} @ {game.home_team_name}")
```

#### Complex Queries

```python
from sqlmodel import Session, select, and_, or_
from mlb_data_platform.models import LiveGameMetadata

with Session(engine) as session:
    # Get all Dodgers home games in October 2024
    statement = select(LiveGameMetadata).where(
        and_(
            LiveGameMetadata.home_team_name == "Los Angeles Dodgers",
            LiveGameMetadata.game_date >= date(2024, 10, 1),
            LiveGameMetadata.game_date < date(2024, 11, 1),
        )
    )

    games = session.exec(statement).all()
```

## Working with Related Tables

### Plays and Pitch Events (Event Type Split)

The `playEvents` array is split into two separate tables:
- **`LiveGamePitchEvents`** - Pitch events (`isPitch=true`)
- **`LiveGamePlayActions`** - Non-pitch events (`isPitch=false`)

#### Querying Pitch Events

```python
from sqlmodel import Session, select
from mlb_data_platform.models import LiveGamePitchEvents

with Session(engine) as session:
    # Get all pitches for a specific game
    statement = select(LiveGamePitchEvents).where(
        LiveGamePitchEvents.game_pk == 747175
    )

    pitches = session.exec(statement).all()

    print(f"Total pitches: {len(pitches)}")

    # Analyze pitch types
    for pitch in pitches:
        print(f"Pitch #{pitch.pitch_number}: {pitch.pitch_type_description} - {pitch.start_speed} MPH")
```

#### Querying Play Actions (Non-Pitch Events)

```python
from sqlmodel import Session, select
from mlb_data_platform.models import LiveGamePlayActions

with Session(engine) as session:
    # Get all substitutions
    statement = select(LiveGamePlayActions).where(
        and_(
            LiveGamePlayActions.game_pk == 747175,
            LiveGamePlayActions.is_substitution == True,
        )
    )

    substitutions = session.exec(statement).all()

    for sub in substitutions:
        print(f"Substitution: Player {sub.player_in_id} in for {sub.player_out_id} at {sub.position}")
```

### Fielding Credits

The new `LiveGameFieldingCredits` table (triple-nested from `plays → playEvents → credits`):

```python
from sqlmodel import Session, select
from mlb_data_platform.models import LiveGameFieldingCredits

with Session(engine) as session:
    # Get all putouts for a game
    statement = select(LiveGameFieldingCredits).where(
        and_(
            LiveGameFieldingCredits.game_pk == 747175,
            LiveGameFieldingCredits.credit_type == "f_putout",
        )
    )

    putouts = session.exec(statement).all()

    print(f"Total putouts: {len(putouts)}")

    for credit in putouts:
        print(f"Player {credit.player_id} ({credit.position_code}): {credit.credit_type}")
```

## Advanced Queries

### Aggregations

```python
from sqlmodel import Session, select, func
from mlb_data_platform.models import LiveGamePitchEvents

with Session(engine) as session:
    # Average pitch speed by pitch type
    statement = select(
        LiveGamePitchEvents.pitch_type_description,
        func.avg(LiveGamePitchEvents.start_speed).label("avg_speed"),
        func.count(LiveGamePitchEvents.pitch_number).label("pitch_count"),
    ).group_by(
        LiveGamePitchEvents.pitch_type_description
    ).order_by(
        func.avg(LiveGamePitchEvents.start_speed).desc()
    )

    results = session.exec(statement).all()

    for pitch_type, avg_speed, count in results:
        print(f"{pitch_type}: {avg_speed:.1f} MPH ({count} pitches)")
```

### Joins

```python
from sqlmodel import Session, select
from mlb_data_platform.models import LiveGamePlays, LiveGamePlayers

with Session(engine) as session:
    # Get all plays with batter names
    statement = select(
        LiveGamePlays,
        LiveGamePlayers.full_name.label("batter_name")
    ).join(
        LiveGamePlayers,
        and_(
            LiveGamePlays.game_pk == LiveGamePlayers.game_pk,
            LiveGamePlays.batter_id == LiveGamePlayers.player_id,
        )
    ).where(
        LiveGamePlays.game_pk == 747175
    )

    results = session.exec(statement).all()

    for play, batter_name in results:
        print(f"{batter_name}: {play.event} ({play.description})")
```

### Batch Inserts

```python
from sqlmodel import Session
from mlb_data_platform.models import LiveGamePlayers

with Session(engine) as session:
    players = [
        LiveGamePlayers(
            game_pk=747175,
            player_id=123,
            full_name="Shohei Ohtani",
            primary_position_code="DH",
        ),
        LiveGamePlayers(
            game_pk=747175,
            player_id=456,
            full_name="Freddie Freeman",
            primary_position_code="1B",
        ),
    ]

    session.add_all(players)
    session.commit()
```

## Using ORM in PySpark Transformations

You can use ORM models to define schemas for PySpark DataFrames:

```python
from pyspark.sql import SparkSession
from mlb_data_platform.models import LiveGameMetadata
from pydantic import TypeAdapter

# Create Spark schema from SQLModel
def sqlmodel_to_spark_schema(model_class):
    """Convert SQLModel to PySpark schema."""
    # Use Pydantic to get field info
    adapter = TypeAdapter(model_class)
    # ... conversion logic
    return spark_schema

# Use in transformation
spark = SparkSession.builder.getOrCreate()

# Read from database
df = spark.read.jdbc(
    url="jdbc:postgresql://localhost:5432/mlb_games",
    table="game.live_game_metadata",
)

# Validate using ORM model
for row in df.collect():
    game = LiveGameMetadata(**row.asDict())
    # Pydantic validation ensures data integrity
    print(game.game_pk, game.home_team_name)
```

## Best Practices

### 1. Use Type Hints

```python
from mlb_data_platform.models import LiveGameMetadata
from typing import List

def get_games_by_team(session, team_name: str) -> List[LiveGameMetadata]:
    """Get all games for a team."""
    statement = select(LiveGameMetadata).where(
        or_(
            LiveGameMetadata.home_team_name == team_name,
            LiveGameMetadata.away_team_name == team_name,
        )
    )
    return session.exec(statement).all()
```

### 2. Use Context Managers

```python
# ✅ Good - automatic session cleanup
with Session(engine) as session:
    game = session.exec(select(LiveGameMetadata).where(...)).first()

# ❌ Bad - manual session management
session = Session(engine)
game = session.exec(select(LiveGameMetadata).where(...)).first()
session.close()  # Easy to forget!
```

### 3. Handle None Values

```python
from mlb_data_platform.models import LiveGameMetadata

game = session.exec(select(LiveGameMetadata).where(...)).first()

# ✅ Good - check for None
if game:
    print(game.home_team_name)
else:
    print("Game not found")

# ❌ Bad - might raise AttributeError
print(game.home_team_name)  # Crashes if game is None
```

### 4. Use Bulk Operations

```python
# ✅ Good - single transaction
with Session(engine) as session:
    session.add_all(games)
    session.commit()

# ❌ Bad - multiple transactions
for game in games:
    with Session(engine) as session:
        session.add(game)
        session.commit()
```

## Regenerating Models

When you update the YAML schema mapping, regenerate the ORM models:

```bash
# Regenerate ORM models
uv run python -m mlb_data_platform.schema.orm_generator \
    config/schemas/mappings/game/live_game_v1.yaml \
    > src/mlb_data_platform/models/game_live.py
```

## Testing with ORM Models

```python
import pytest
from sqlmodel import Session, create_engine, SQLModel
from mlb_data_platform.models import LiveGameMetadata

@pytest.fixture
def session():
    """Create in-memory test database."""
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        yield session

def test_create_game(session):
    """Test creating a game."""
    game = LiveGameMetadata(
        game_pk=1,
        game_date=date(2024, 10, 25),
        home_team_name="Test Home",
        away_team_name="Test Away",
    )

    session.add(game)
    session.commit()

    # Query it back
    result = session.get(LiveGameMetadata, 1)
    assert result.game_pk == 1
    assert result.home_team_name == "Test Home"
```

## See Also

- [SQLModel Documentation](https://sqlmodel.tiangolo.com/)
- [Pydantic Documentation](https://docs.pydantic.dev/)
- [SQLAlchemy 2.0 Documentation](https://docs.sqlalchemy.org/en/20/)
- `src/mlb_data_platform/models/game_live.py` - Auto-generated ORM models
- `config/schemas/mappings/game/live_game_v1.yaml` - Schema source
