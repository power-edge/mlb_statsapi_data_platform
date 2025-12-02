"""Pipeline orchestration for MLB data platform.

This module provides hierarchical pipeline orchestration that connects:
- Season data (dates, schedule boundaries)
- Schedule data (games per date)
- Game data (live/final game states)
- Entity enrichment (persons, teams)

Key Components:
- SeasonPipeline: Fetches season metadata, drives date-based scheduling
- SchedulePipeline: Fetches daily schedules, extracts game_pks
- GamePipeline: Fetches game data (final or timestamped snapshots)
- LiveGamePoller: Polls for live game updates using timestamp diffs
- EntityEnricher: Fetches person/team data based on game rosters

Architecture:
    Season.seasons → dates[]
        ↓
    Schedule.schedule(date) → game_pks[]
        ↓
    Game.liveGameV1(game_pk) → game data
        ↓
    Person.person(player_ids) → player enrichment
    Team.teams(team_ids) → team enrichment

Live Game Flow:
    1. Poll schedule for games with status='Live' or 'Pre-Game'
    2. For each live game:
       a. Get liveTimestampv11 → available timestamps
       b. Poll liveGameDiffPatchV1(startTimecode=last) → changes
       c. Store timestamped snapshots
    3. On game completion → fetch final liveGameV1 (no timecode)
"""

from mlb_data_platform.pipeline.extractors import (
    GameExtractor,
    ScheduleExtractor,
    SeasonExtractor,
)
from mlb_data_platform.pipeline.live_poller import LiveGamePoller
from mlb_data_platform.pipeline.orchestrator import (
    PipelineConfig,
    PipelineOrchestrator,
    PipelineResult,
)
from mlb_data_platform.pipeline.storage_adapter import (
    StorageAdapter,
    create_storage_callback,
)

__all__ = [
    "PipelineOrchestrator",
    "PipelineConfig",
    "PipelineResult",
    "SeasonExtractor",
    "ScheduleExtractor",
    "GameExtractor",
    "LiveGamePoller",
    "StorageAdapter",
    "create_storage_callback",
]
