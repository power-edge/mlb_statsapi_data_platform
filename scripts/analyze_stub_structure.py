#!/usr/bin/env python3
"""Comprehensive analysis of game.liveGameV1 stub data structure.

This script analyzes the JSON structure to identify:
- All arrays that could become tables
- Nested objects
- Field types and cardinalities
- Potential table splits (e.g., pitch events vs non-pitch events)
"""

import gzip
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set


def analyze_value(value: Any, path: str, stats: Dict):
    """Recursively analyze JSON structure."""
    if isinstance(value, dict):
        stats["objects"].add(path)
        for key, val in value.items():
            analyze_value(val, f"{path}.{key}", stats)

    elif isinstance(value, list):
        stats["arrays"].add(path)
        if value:
            # Analyze first element to understand array structure
            first = value[0]
            if isinstance(first, dict):
                stats["array_of_objects"].add(path)
                stats["array_sizes"][path] = len(value)
                # Analyze object structure
                for key in first.keys():
                    stats["array_object_keys"][path].add(key)
            elif isinstance(first, (int, str)):
                stats["array_of_primitives"].add(path)
                stats["array_sizes"][path] = len(value)

            # Analyze all elements
            for item in value:
                analyze_value(item, f"{path}[]", stats)

    else:
        # Leaf value (string, int, bool, etc.)
        stats["leaves"].add(path)
        stats["types"][path] = type(value).__name__


def main():
    # Load stub data
    stub_file = Path.home() / "github.com/power-edge/pymlb_statsapi/tests/bdd/stubs/game/liveGameV1/liveGameV1_game_pk=747175_4240fc08a038.json.gz"

    with gzip.open(stub_file, "rt") as f:
        stub_wrapper = json.load(f)

    data = stub_wrapper["response"]

    # Statistics
    stats = {
        "objects": set(),
        "arrays": set(),
        "array_of_objects": set(),
        "array_of_primitives": set(),
        "array_sizes": {},
        "array_object_keys": defaultdict(set),
        "leaves": set(),
        "types": {},
    }

    # Analyze
    analyze_value(data, "$", stats)

    # Report
    print("=" * 80)
    print("GAME.LIVEGAMEV1 STRUCTURE ANALYSIS")
    print("=" * 80)

    print("\n📊 ARRAYS (potential tables)")
    print("-" * 80)
    arrays_sorted = sorted(stats["array_of_objects"], key=lambda x: stats["array_sizes"].get(x, 0), reverse=True)
    for array_path in arrays_sorted:
        size = stats["array_sizes"].get(array_path, 0)
        keys = stats["array_object_keys"].get(array_path, set())
        print(f"\n{array_path}")
        print(f"  Count: {size} items")
        print(f"  Keys: {', '.join(sorted(keys)[:10])}")
        if len(keys) > 10:
            print(f"       ... and {len(keys) - 10} more")

    print("\n\n📋 PRIMITIVE ARRAYS (simple lists)")
    print("-" * 80)
    for array_path in sorted(stats["array_of_primitives"]):
        size = stats["array_sizes"].get(array_path, 0)
        print(f"{array_path}: {size} items")

    print("\n\n🔍 SPECIAL CASES TO INVESTIGATE")
    print("-" * 80)

    # Check playEvents for pitch vs non-pitch
    plays_path = "$.liveData.plays.allPlays"
    if plays_path in stats["array_of_objects"]:
        print(f"\n1. Play Events (pitch vs non-pitch events)")
        print(f"   Path: $.liveData.plays.allPlays[].playEvents[]")
        print(f"   - Check isPitch field to split into:")
        print(f"     • pitch_events table (isPitch=true)")
        print(f"     • play_action_events table (isPitch=false, actions/substitutions/etc.)")

    # Check for runners
    print(f"\n2. Runners (base runner movements)")
    print(f"   Path: $.liveData.plays.allPlays[].runners[]")
    print(f"   - Each play can have multiple runners")
    print(f"   - Includes movement, credits, details")

    # Check for credits
    print(f"\n3. Credits (fielding/pitching credits per play)")
    print(f"   Path: $.liveData.plays.allPlays[].playEvents[].credits[]")
    print(f"   - Defensive credits (putouts, assists, errors)")

    # Check for reviews
    print(f"\n4. Reviews (replay reviews)")
    print(f"   Path: $.liveData.plays.allPlays[].reviewDetails")
    print(f"   - Challenge/review information")

    # Check for box score details
    print(f"\n5. Box Score Player Stats")
    print(f"   Path: $.liveData.boxscore.teams.[home|away].players.ID*.stats")
    print(f"   - Batting stats, pitching stats per player")
    print(f"   - Could be separate tables for batting/pitching stats")

    # Check for game events timeline
    print(f"\n6. Game Timeline Events")
    print(f"   Path: Check if there's a timeline/feed of all game events")

    # Save detailed report
    output_file = Path("docs/STUB_DATA_ANALYSIS.md")
    with open(output_file, "w") as f:
        f.write("# Game.liveGameV1 Stub Data Structure Analysis\n\n")
        f.write(f"**Analyzed**: {stub_file.name}\n")
        f.write(f"**Game PK**: 747175\n\n")

        f.write("## Summary Statistics\n\n")
        f.write(f"- Total objects: {len(stats['objects'])}\n")
        f.write(f"- Total arrays: {len(stats['arrays'])}\n")
        f.write(f"- Arrays of objects: {len(stats['array_of_objects'])}\n")
        f.write(f"- Arrays of primitives: {len(stats['array_of_primitives'])}\n\n")

        f.write("## Arrays (Potential Tables)\n\n")
        f.write("| Path | Count | Sample Keys |\n")
        f.write("|------|-------|-------------|\n")
        for array_path in arrays_sorted:
            size = stats["array_sizes"].get(array_path, 0)
            keys = sorted(stats["array_object_keys"].get(array_path, set()))[:5]
            f.write(f"| `{array_path}` | {size} | {', '.join(keys)} |\n")

        f.write("\n## All Discovered Paths\n\n")
        f.write("```\n")
        for path in sorted(stats["arrays"]):
            f.write(f"{path}\n")
        f.write("```\n")

    print(f"\n\n✅ Detailed report written to: {output_file}")
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
