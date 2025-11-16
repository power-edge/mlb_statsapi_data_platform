# Game.liveGameV1 Stub Data Structure Analysis

**Analyzed**: liveGameV1_game_pk=747175_4240fc08a038.json.gz
**Game PK**: 747175

## Summary Statistics

- Total objects: 1101
- Total arrays: 77
- Arrays of objects: 49
- Arrays of primitives: 20

## Arrays (Potential Tables)

| Path | Count | Sample Keys |
|------|-------|-------------|
| `$.liveData.plays.allPlays` | 75 | about, actionIndex, atBatIndex, count, matchup |
| `$.liveData.boxscore.info` | 13 | label, value |
| `$.liveData.plays.playsByInning` | 9 | bottom, endIndex, hits, startIndex, top |
| `$.liveData.linescore.innings` | 9 | away, home, num, ordinalNum |
| `$.liveData.boxscore.teams.away.info[].fieldList` | 9 | label, value |
| `$.liveData.boxscore.officials` | 4 | official, officialType |
| `$.liveData.boxscore.teams.home.info` | 3 | fieldList, title |
| `$.liveData.boxscore.topPerformers` | 3 | gameScore, pitchingGameScore, player, type |
| `$.liveData.boxscore.teams.away.players.ID687462.allPositions` | 2 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.home.players.ID606466.allPositions` | 2 | abbreviation, code, name, type |
| `$.liveData.plays.allPlays[].playEvents` | 2 | battingOrder, count, details, endTime, hitData |
| `$.liveData.plays.currentPlay.runners` | 2 | credits, details, movement |
| `$.liveData.plays.playsByInning[].hits.away` | 2 | batter, coordinates, description, inning, pitcher |
| `$.liveData.plays.currentPlay.playEvents` | 2 | count, details, endTime, index, isPitch |
| `$.liveData.plays.allPlays[].runners` | 2 | credits, details, movement |
| `$.liveData.plays.currentPlay.runners[].credits` | 1 | credit, player, position |
| `$.liveData.boxscore.teams.away.players.ID670950.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.plays.allPlays[].runners[].credits` | 1 | credit, player, position |
| `$.liveData.boxscore.teams.away.players.ID662139.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.away.players.ID663893.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.home.players.ID592626.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.away.players.ID650893.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.home.players.ID677950.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.home.players.ID682998.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.away.info` | 1 | fieldList, title |
| `$.liveData.boxscore.teams.home.note` | 1 | label, value |
| `$.liveData.plays.playsByInning[].hits.home` | 1 | batter, coordinates, description, inning, pitcher |
| `$.liveData.boxscore.teams.away.players.ID621114.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.away.players.ID684320.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.away.players.ID672386.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.home.players.ID669194.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.home.players.ID553993.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.away.players.ID665489.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.topPerformers[].player.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.home.players.ID672695.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.away.players.ID643338.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.home.players.ID679885.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.home.info[].fieldList` | 1 | label, value |
| `$.liveData.boxscore.teams.home.players.ID686826.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.away.players.ID676391.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.home.players.ID572233.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.away.players.ID595281.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.home.players.ID545341.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.away.players.ID457759.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.home.players.ID621028.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.away.players.ID677870.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.away.players.ID676914.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.home.players.ID672515.allPositions` | 1 | abbreviation, code, name, type |
| `$.liveData.boxscore.teams.away.players.ID543807.allPositions` | 1 | abbreviation, code, name, type |

## All Discovered Paths

```
$.gameData.alerts
$.liveData.boxscore.info
$.liveData.boxscore.officials
$.liveData.boxscore.pitchingNotes
$.liveData.boxscore.teams.away.batters
$.liveData.boxscore.teams.away.battingOrder
$.liveData.boxscore.teams.away.bench
$.liveData.boxscore.teams.away.bullpen
$.liveData.boxscore.teams.away.info
$.liveData.boxscore.teams.away.info[].fieldList
$.liveData.boxscore.teams.away.note
$.liveData.boxscore.teams.away.pitchers
$.liveData.boxscore.teams.away.players.ID457759.allPositions
$.liveData.boxscore.teams.away.players.ID543807.allPositions
$.liveData.boxscore.teams.away.players.ID595281.allPositions
$.liveData.boxscore.teams.away.players.ID621114.allPositions
$.liveData.boxscore.teams.away.players.ID643338.allPositions
$.liveData.boxscore.teams.away.players.ID650893.allPositions
$.liveData.boxscore.teams.away.players.ID662139.allPositions
$.liveData.boxscore.teams.away.players.ID663893.allPositions
$.liveData.boxscore.teams.away.players.ID665489.allPositions
$.liveData.boxscore.teams.away.players.ID670950.allPositions
$.liveData.boxscore.teams.away.players.ID672386.allPositions
$.liveData.boxscore.teams.away.players.ID676391.allPositions
$.liveData.boxscore.teams.away.players.ID676914.allPositions
$.liveData.boxscore.teams.away.players.ID677870.allPositions
$.liveData.boxscore.teams.away.players.ID684320.allPositions
$.liveData.boxscore.teams.away.players.ID687462.allPositions
$.liveData.boxscore.teams.home.batters
$.liveData.boxscore.teams.home.battingOrder
$.liveData.boxscore.teams.home.bench
$.liveData.boxscore.teams.home.bullpen
$.liveData.boxscore.teams.home.info
$.liveData.boxscore.teams.home.info[].fieldList
$.liveData.boxscore.teams.home.note
$.liveData.boxscore.teams.home.pitchers
$.liveData.boxscore.teams.home.players.ID545341.allPositions
$.liveData.boxscore.teams.home.players.ID553993.allPositions
$.liveData.boxscore.teams.home.players.ID572233.allPositions
$.liveData.boxscore.teams.home.players.ID592626.allPositions
$.liveData.boxscore.teams.home.players.ID606466.allPositions
$.liveData.boxscore.teams.home.players.ID621028.allPositions
$.liveData.boxscore.teams.home.players.ID669194.allPositions
$.liveData.boxscore.teams.home.players.ID672515.allPositions
$.liveData.boxscore.teams.home.players.ID672695.allPositions
$.liveData.boxscore.teams.home.players.ID677950.allPositions
$.liveData.boxscore.teams.home.players.ID679885.allPositions
$.liveData.boxscore.teams.home.players.ID682998.allPositions
$.liveData.boxscore.teams.home.players.ID686826.allPositions
$.liveData.boxscore.topPerformers
$.liveData.boxscore.topPerformers[].player.allPositions
$.liveData.linescore.innings
$.liveData.plays.allPlays
$.liveData.plays.allPlays[].actionIndex
$.liveData.plays.allPlays[].matchup.batterHotColdZones
$.liveData.plays.allPlays[].matchup.pitcherHotColdZones
$.liveData.plays.allPlays[].pitchIndex
$.liveData.plays.allPlays[].playEvents
$.liveData.plays.allPlays[].runnerIndex
$.liveData.plays.allPlays[].runners
$.liveData.plays.allPlays[].runners[].credits
$.liveData.plays.currentPlay.actionIndex
$.liveData.plays.currentPlay.matchup.batterHotColdZones
$.liveData.plays.currentPlay.matchup.pitcherHotColdZones
$.liveData.plays.currentPlay.pitchIndex
$.liveData.plays.currentPlay.playEvents
$.liveData.plays.currentPlay.runnerIndex
$.liveData.plays.currentPlay.runners
$.liveData.plays.currentPlay.runners[].credits
$.liveData.plays.playsByInning
$.liveData.plays.playsByInning[].bottom
$.liveData.plays.playsByInning[].hits.away
$.liveData.plays.playsByInning[].hits.home
$.liveData.plays.playsByInning[].top
$.liveData.plays.scoringPlays
$.metaData.gameEvents
$.metaData.logicalEvents
```
