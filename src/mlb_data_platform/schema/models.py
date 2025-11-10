"""Schema metadata models for tracking relationships and constraints."""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class RelationshipType(str, Enum):
    """Types of relationships between schemas."""

    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class FieldMetadata(BaseModel):
    """Metadata about a field in a schema."""

    name: str
    type: str  # Avro type or SQL type
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_partition_key: bool = False
    is_indexed: bool = False
    description: Optional[str] = None
    json_path: Optional[str] = None  # JSONPath to extract from raw data
    default_value: Optional[Any] = None


class SchemaRelationship(BaseModel):
    """Defines a relationship between two schemas."""

    from_schema: str = Field(..., description="Source schema name (e.g., 'game.live_game_plays')")
    to_schema: str = Field(..., description="Target schema name (e.g., 'game.live_game_v1')")
    relationship_type: RelationshipType
    from_field: str = Field(..., description="Field name in source schema")
    to_field: str = Field(..., description="Field name in target schema")
    description: Optional[str] = None
    cascade_delete: bool = False


class SchemaMetadata(BaseModel):
    """Complete metadata for a schema including fields and relationships."""

    # Schema identification
    endpoint: str = Field(..., description="MLB Stats API endpoint (e.g., 'game', 'schedule')")
    method: str = Field(..., description="Endpoint method (e.g., 'liveGameV1', 'schedule')")
    schema_name: str = Field(..., description="Full table name (e.g., 'game.live_game_v1')")
    version: str = Field(default="v1", description="Schema version")

    # Schema structure
    fields: list[FieldMetadata] = Field(default_factory=list)
    primary_keys: list[str] = Field(default_factory=list)
    partition_keys: list[str] = Field(default_factory=list)

    # Relationships
    relationships: list[SchemaRelationship] = Field(default_factory=list)

    # Avro schema (JSON representation)
    avro_schema: Optional[dict[str, Any]] = None

    # Additional metadata
    description: Optional[str] = None
    source_json_schema: Optional[dict[str, Any]] = None  # Original pymlb_statsapi JSON schema
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def get_primary_key_fields(self) -> list[FieldMetadata]:
        """Get all fields marked as primary keys."""
        return [f for f in self.fields if f.is_primary_key]

    def get_foreign_key_fields(self) -> list[FieldMetadata]:
        """Get all fields marked as foreign keys."""
        return [f for f in self.fields if f.is_foreign_key]

    def get_partition_fields(self) -> list[FieldMetadata]:
        """Get all fields marked as partition keys."""
        return [f for f in self.fields if f.is_partition_key]

    def get_relationship_by_field(self, field_name: str) -> Optional[SchemaRelationship]:
        """Get relationship for a specific field."""
        return next((r for r in self.relationships if r.from_field == field_name), None)


# Predefined schema metadata for core MLB Stats API endpoints
# These define the raw table structures with their relationships

SCHEMA_METADATA_REGISTRY: dict[str, SchemaMetadata] = {
    # Schedule.schedule() -> schedule.schedule
    "schedule.schedule": SchemaMetadata(
        endpoint="schedule",
        method="schedule",
        schema_name="schedule.schedule",
        description="Daily game schedule from Schedule.schedule()",
        fields=[
            FieldMetadata(
                name="id", type="bigserial", is_primary_key=True, nullable=False
            ),
            FieldMetadata(name="request_params", type="jsonb", nullable=False),
            FieldMetadata(name="source_url", type="text", nullable=False),
            FieldMetadata(name="captured_at", type="timestamptz", nullable=False, is_indexed=True),
            FieldMetadata(name="schema_version", type="varchar(10)", nullable=False),
            FieldMetadata(name="response_status", type="int", nullable=False),
            FieldMetadata(name="data", type="jsonb", nullable=False),
            # Extracted fields for indexing
            FieldMetadata(
                name="sport_id",
                type="int",
                json_path="$.request_params.sportId",
                is_indexed=True,
            ),
            FieldMetadata(
                name="schedule_date",
                type="date",
                json_path="$.request_params.date",
                is_partition_key=True,
                is_indexed=True,
                nullable=False,
            ),
            FieldMetadata(name="season", type="varchar(10)", is_indexed=True),
            FieldMetadata(name="ingestion_timestamp", type="timestamptz"),
            FieldMetadata(name="ingestion_job_id", type="varchar(100)"),
        ],
        primary_keys=["id"],
        partition_keys=["schedule_date"],
        relationships=[],
    ),
    # Game.liveGameV1() -> game.live_game_v1
    "game.live_game_v1": SchemaMetadata(
        endpoint="game",
        method="liveGameV1",
        schema_name="game.live_game_v1",
        description="Complete live game feed from Game.liveGameV1() - MASTER table",
        fields=[
            FieldMetadata(name="id", type="bigserial", is_primary_key=True, nullable=False),
            FieldMetadata(name="request_params", type="jsonb", nullable=False),
            FieldMetadata(name="source_url", type="text", nullable=False),
            FieldMetadata(name="captured_at", type="timestamptz", nullable=False, is_indexed=True),
            FieldMetadata(name="schema_version", type="varchar(10)", nullable=False),
            FieldMetadata(name="response_status", type="int", nullable=False),
            FieldMetadata(name="data", type="jsonb", nullable=False),
            # Extracted fields
            FieldMetadata(
                name="game_pk",
                type="bigint",
                json_path="$.gamePk",
                is_indexed=True,
                nullable=False,
            ),
            FieldMetadata(
                name="game_date",
                type="date",
                json_path="$.gameData.datetime.officialDate",
                is_partition_key=True,
                is_indexed=True,
                nullable=False,
            ),
            FieldMetadata(
                name="season",
                type="varchar(10)",
                json_path="$.gameData.game.season",
                is_indexed=True,
            ),
            FieldMetadata(
                name="game_type", type="varchar(5)", json_path="$.gameData.game.type"
            ),
            FieldMetadata(
                name="game_state",
                type="varchar(20)",
                json_path="$.gameData.status.abstractGameState",
                is_indexed=True,
            ),
            FieldMetadata(
                name="home_team_id", type="int", json_path="$.gameData.teams.home.id"
            ),
            FieldMetadata(
                name="away_team_id", type="int", json_path="$.gameData.teams.away.id"
            ),
            FieldMetadata(name="venue_id", type="int", json_path="$.gameData.venue.id"),
            FieldMetadata(
                name="timecode",
                type="varchar(20)",
                description="Optional timecode for historical snapshots",
            ),
            FieldMetadata(
                name="is_latest",
                type="boolean",
                default_value=True,
                is_indexed=True,
                description="Only one row per game_pk should be TRUE",
            ),
            FieldMetadata(name="ingestion_timestamp", type="timestamptz"),
            FieldMetadata(name="ingestion_job_id", type="varchar(100)"),
        ],
        primary_keys=["id", "game_date"],
        partition_keys=["game_date"],
        relationships=[
            # schedule.games references this table
            SchemaRelationship(
                from_schema="schedule.games",
                to_schema="game.live_game_v1",
                relationship_type=RelationshipType.MANY_TO_ONE,
                from_field="game_pk",
                to_field="game_pk",
                description="Games in schedule reference live game data",
            )
        ],
    ),
    # Season.seasons() -> season.seasons
    "season.seasons": SchemaMetadata(
        endpoint="season",
        method="seasons",
        schema_name="season.seasons",
        description="Season data from Season.seasons()",
        fields=[
            FieldMetadata(name="id", type="bigserial", is_primary_key=True, nullable=False),
            FieldMetadata(name="request_params", type="jsonb", nullable=False),
            FieldMetadata(name="source_url", type="text", nullable=False),
            FieldMetadata(name="captured_at", type="timestamptz", nullable=False, is_indexed=True),
            FieldMetadata(name="schema_version", type="varchar(10)", nullable=False),
            FieldMetadata(name="response_status", type="int", nullable=False),
            FieldMetadata(name="data", type="jsonb", nullable=False),
            # Extracted fields
            FieldMetadata(
                name="sport_id",
                type="int",
                json_path="$.request_params.sportId",
                is_indexed=True,
            ),
            FieldMetadata(
                name="season_id", type="varchar(10)", json_path="$.request_params.season"
            ),
            FieldMetadata(name="ingestion_timestamp", type="timestamptz"),
            FieldMetadata(name="ingestion_job_id", type="varchar(100)"),
        ],
        primary_keys=["id"],
        partition_keys=[],
        relationships=[],
    ),
}
