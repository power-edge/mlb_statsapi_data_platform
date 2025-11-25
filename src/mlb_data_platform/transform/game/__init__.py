"""Game transformation modules.

Contains transformation classes for all game-related endpoints:
- LiveGameTransform: Game.liveGameV1() → 16 normalized tables
"""

from .live_game import LiveGameTransform

__all__ = ["LiveGameTransform"]
