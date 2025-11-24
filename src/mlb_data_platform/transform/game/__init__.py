"""Game transformation modules.

Contains transformation classes for all game-related endpoints:
- LiveGameTransformation: Game.liveGameV1() → normalized tables
- GameLiveV1Transformation: Legacy comprehensive transformation (17 tables)
"""

from .live_game import LiveGameTransformation

__all__ = ["LiveGameTransformation"]
