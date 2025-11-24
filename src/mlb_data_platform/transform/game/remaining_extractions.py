"""Remaining extraction implementations for game transform.

These methods will be copied into live_game_v2.py.
"""

# LINESCORE
def _extract_linescore(self, raw_df: DataFrame) -> DataFrame:
    """Extract inning-by-inning linescore."""
    schema = StructType([
        StructField("gamePk", IntegerType(), True),
        StructField("liveData", StructType([
            StructField("linescore", StructType([
                StructField("innings", ArrayType(StructType([
                    StructField("num", IntegerType(), True),
                    StructField("ordinalNum", StringType(), True),
                    StructField("home", StructType([
                        StructField("runs", IntegerType(), True),
                        StructField("hits", IntegerType(), True),
                        StructField("errors", IntegerType(), True),
                        StructField("leftOnBase", IntegerType(), True),
                    ]), True),
                    StructField("away", StructType([
                        StructField("runs", IntegerType(), True),
                        StructField("hits", IntegerType(), True),
                        StructField("errors", IntegerType(), True),
                        StructField("leftOnBase", IntegerType(), True),
                    ]), True),
                ])), True),
            ]), True),
        ]), True),
    ])

    parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

    innings_df = parsed_df.select(
        F.col("parsed.gamePk").alias("game_pk"),
        F.explode(F.col("parsed.liveData.linescore.innings")).alias("inning")
    )

    linescore_df = innings_df.select(
        F.col("game_pk"),
        F.col("inning.num").alias("inning_number"),
        F.col("inning.ordinalNum").alias("inning_ordinal"),
        F.col("inning.home.runs").alias("home_runs"),
        F.col("inning.home.hits").alias("home_hits"),
        F.col("inning.home.errors").alias("home_errors"),
        F.col("inning.home.leftOnBase").alias("home_left_on_base"),
        F.col("inning.away.runs").alias("away_runs"),
        F.col("inning.away.hits").alias("away_hits"),
        F.col("inning.away.errors").alias("away_errors"),
        F.col("inning.away.leftOnBase").alias("away_left_on_base"),
        F.current_timestamp().alias("source_captured_at"),
    )

    return linescore_df


# PLAYER REGISTRY (uses dynamic map)
def _extract_all_players(self, raw_df: DataFrame) -> DataFrame:
    """Extract all players registry with stats."""
    from pyspark.sql.types import MapType

    schema = StructType([
        StructField("gamePk", IntegerType(), True),
        StructField("liveData", StructType([
            StructField("boxscore", StructType([
                StructField("teams", StructType([
                    StructField("home", StructType([
                        StructField("players", MapType(StringType(), StructType([
                            StructField("person", StructType([
                                StructField("id", IntegerType(), True),
                                StructField("fullName", StringType(), True),
                            ]), True),
                            StructField("jerseyNumber", StringType(), True),
                            StructField("position", StructType([
                                StructField("code", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("type", StringType(), True),
                                StructField("abbreviation", StringType(), True),
                            ]), True),
                            StructField("status", StructType([
                                StructField("code", StringType(), True),
                                StructField("description", StringType(), True),
                            ]), True),
                        ])), True),
                    ]), True),
                    StructField("away", StructType([
                        StructField("players", MapType(StringType(), StructType([
                            StructField("person", StructType([
                                StructField("id", IntegerType(), True),
                                StructField("fullName", StringType(), True),
                            ]), True),
                            StructField("jerseyNumber", StringType(), True),
                            StructField("position", StructType([
                                StructField("code", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("type", StringType(), True),
                                StructField("abbreviation", StringType(), True),
                            ]), True),
                            StructField("status", StructType([
                                StructField("code", StringType(), True),
                                StructField("description", StringType(), True),
                            ]), True),
                        ])), True),
                    ]), True),
                ]), True),
            ]), True),
        ]), True),
    ])

    parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

    # Extract home players
    home_players = parsed_df.select(
        F.col("parsed.gamePk").alias("game_pk"),
        F.lit("home").alias("team_side"),
        F.explode(F.col("parsed.liveData.boxscore.teams.home.players")).alias("player_key", "player")
    )

    # Extract away players
    away_players = parsed_df.select(
        F.col("parsed.gamePk").alias("game_pk"),
        F.lit("away").alias("team_side"),
        F.explode(F.col("parsed.liveData.boxscore.teams.away.players")).alias("player_key", "player")
    )

    # Union and extract fields
    all_players = home_players.union(away_players).select(
        F.col("game_pk"),
        F.col("team_side"),
        F.col("player.person.id").alias("player_id"),
        F.col("player.person.fullName").alias("full_name"),
        F.col("player.jerseyNumber").alias("jersey_number"),
        F.col("player.position.code").alias("position_code"),
        F.col("player.position.name").alias("position_name"),
        F.col("player.position.type").alias("position_type"),
        F.col("player.position.abbreviation").alias("position_abbrev"),
        F.col("player.status.code").alias("status_code"),
        F.col("player.status.description").alias("status_description"),
        F.current_timestamp().alias("source_captured_at"),
    )

    return all_players


# VENUE DETAILS
def _extract_venue_details(self, raw_df: DataFrame) -> DataFrame:
    """Extract detailed venue information."""
    schema = StructType([
        StructField("gamePk", IntegerType(), True),
        StructField("gameData", StructType([
            StructField("venue", StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("location", StructType([
                    StructField("city", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("stateAbbrev", StringType(), True),
                ]), True),
                StructField("timeZone", StructType([
                    StructField("id", StringType(), True),
                    StructField("offset", IntegerType(), True),
                    StructField("tz", StringType(), True),
                ]), True),
                StructField("fieldInfo", StructType([
                    StructField("capacity", IntegerType(), True),
                    StructField("turfType", StringType(), True),
                    StructField("roofType", StringType(), True),
                    StructField("leftLine", IntegerType(), True),
                    StructField("leftCenter", IntegerType(), True),
                    StructField("center", IntegerType(), True),
                    StructField("rightCenter", IntegerType(), True),
                    StructField("rightLine", IntegerType(), True),
                ]), True),
            ]), True),
        ]), True),
    ])

    parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

    venue_df = parsed_df.select(
        F.col("parsed.gamePk").alias("game_pk"),
        F.col("parsed.gameData.venue.id").alias("venue_id"),
        F.col("parsed.gameData.venue.name").alias("venue_name"),
        F.col("parsed.gameData.venue.location.city").alias("city"),
        F.col("parsed.gameData.venue.location.state").alias("state"),
        F.col("parsed.gameData.venue.location.stateAbbrev").alias("state_abbrev"),
        F.col("parsed.gameData.venue.timeZone.id").alias("timezone_id"),
        F.col("parsed.gameData.venue.timeZone.offset").alias("timezone_offset"),
        F.col("parsed.gameData.venue.timeZone.tz").alias("timezone_tz"),
        F.col("parsed.gameData.venue.fieldInfo.capacity").alias("capacity"),
        F.col("parsed.gameData.venue.fieldInfo.turfType").alias("turf_type"),
        F.col("parsed.gameData.venue.fieldInfo.roofType").alias("roof_type"),
        F.col("parsed.gameData.venue.fieldInfo.leftLine").alias("left_line"),
        F.col("parsed.gameData.venue.fieldInfo.leftCenter").alias("left_center"),
        F.col("parsed.gameData.venue.fieldInfo.center").alias("center"),
        F.col("parsed.gameData.venue.fieldInfo.rightCenter").alias("right_center"),
        F.col("parsed.gameData.venue.fieldInfo.rightLine").alias("right_line"),
        F.current_timestamp().alias("source_captured_at"),
    )

    return venue_df


# UMPIRES
def _extract_umpires(self, raw_df: DataFrame) -> DataFrame:
    """Extract umpire assignments."""
    schema = StructType([
        StructField("gamePk", IntegerType(), True),
        StructField("liveData", StructType([
            StructField("boxscore", StructType([
                StructField("officials", ArrayType(StructType([
                    StructField("official", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("fullName", StringType(), True),
                    ]), True),
                    StructField("officialType", StringType(), True),
                ])), True),
            ]), True),
        ]), True),
    ])

    parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

    umpires_df = parsed_df.select(
        F.col("parsed.gamePk").alias("game_pk"),
        F.explode(F.col("parsed.liveData.boxscore.officials")).alias("umpire")
    )

    umpires_extracted = umpires_df.select(
        F.col("game_pk"),
        F.col("umpire.official.id").alias("umpire_id"),
        F.col("umpire.official.fullName").alias("full_name"),
        F.col("umpire.officialType").alias("position"),
        F.current_timestamp().alias("source_captured_at"),
    )

    return umpires_extracted


# PLAY ACTIONS
def _extract_play_actions(self, raw_df: DataFrame) -> DataFrame:
    """Extract play actions (non-pitch events)."""
    schema = StructType([
        StructField("gamePk", IntegerType(), True),
        StructField("liveData", StructType([
            StructField("plays", StructType([
                StructField("allPlays", ArrayType(StructType([
                    StructField("about", StructType([
                        StructField("atBatIndex", IntegerType(), True),
                    ]), True),
                    StructField("playEvents", ArrayType(StructType([
                        StructField("isPitch", BooleanType(), True),
                        StructField("type", StringType(), True),
                        StructField("index", IntegerType(), True),
                        StructField("startTime", StringType(), True),
                        StructField("endTime", StringType(), True),
                        StructField("details", StructType([
                            StructField("event", StringType(), True),
                            StructField("eventType", StringType(), True),
                            StructField("description", StringType(), True),
                            StructField("awayScore", IntegerType(), True),
                            StructField("homeScore", IntegerType(), True),
                        ]), True),
                        StructField("player", StructType([
                            StructField("id", IntegerType(), True),
                        ]), True),
                    ])), True),
                ])), True),
            ]), True),
        ]), True),
    ])

    parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

    plays_df = parsed_df.select(
        F.col("parsed.gamePk").alias("game_pk"),
        F.explode(F.col("parsed.liveData.plays.allPlays")).alias("play")
    )

    actions_df = plays_df.select(
        F.col("game_pk"),
        F.col("play.about.atBatIndex").alias("at_bat_index"),
        F.explode(F.col("play.playEvents")).alias("event")
    ).filter(F.col("event.isPitch") == False)

    actions_extracted = actions_df.select(
        F.col("game_pk"),
        F.col("at_bat_index"),
        F.col("event.index").alias("event_index"),
        F.col("event.type").alias("event_type"),
        F.col("event.startTime").cast(TimestampType()).alias("start_time"),
        F.col("event.endTime").cast(TimestampType()).alias("end_time"),
        F.col("event.details.event").alias("event"),
        F.col("event.details.eventType").alias("event_type_code"),
        F.col("event.details.description").alias("description"),
        F.col("event.details.awayScore").alias("away_score"),
        F.col("event.details.homeScore").alias("home_score"),
        F.col("event.player.id").alias("player_id"),
        F.current_timestamp().alias("source_captured_at"),
    )

    return actions_extracted


# RUNNERS
def _extract_runners(self, raw_df: DataFrame) -> DataFrame:
    """Extract base runner movements."""
    schema = StructType([
        StructField("gamePk", IntegerType(), True),
        StructField("liveData", StructType([
            StructField("plays", StructType([
                StructField("allPlays", ArrayType(StructType([
                    StructField("about", StructType([
                        StructField("atBatIndex", IntegerType(), True),
                    ]), True),
                    StructField("runners", ArrayType(StructType([
                        StructField("movement", StructType([
                            StructField("originBase", StringType(), True),
                            StructField("start", StringType(), True),
                            StructField("end", StringType(), True),
                            StructField("outBase", StringType(), True),
                            StructField("isOut", BooleanType(), True),
                            StructField("outNumber", IntegerType(), True),
                        ]), True),
                        StructField("details", StructType([
                            StructField("event", StringType(), True),
                            StructField("eventType", StringType(), True),
                            StructField("runner", StructType([
                                StructField("id", IntegerType(), True),
                                StructField("fullName", StringType(), True),
                            ]), True),
                            StructField("responsiblePitcher", StructType([
                                StructField("id", IntegerType(), True),
                            ]), True),
                            StructField("isScoringEvent", BooleanType(), True),
                            StructField("rbi", BooleanType(), True),
                            StructField("earned", BooleanType(), True),
                            StructField("teamUnearned", BooleanType(), True),
                        ]), True),
                    ])), True),
                ])), True),
            ]), True),
        ]), True),
    ])

    parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

    plays_df = parsed_df.select(
        F.col("parsed.gamePk").alias("game_pk"),
        F.explode(F.col("parsed.liveData.plays.allPlays")).alias("play")
    )

    runners_df = plays_df.select(
        F.col("game_pk"),
        F.col("play.about.atBatIndex").alias("at_bat_index"),
        F.explode(F.col("play.runners")).alias("runner")
    )

    runners_extracted = runners_df.select(
        F.col("game_pk"),
        F.col("at_bat_index"),
        F.col("runner.details.runner.id").alias("runner_id"),
        F.col("runner.details.runner.fullName").alias("runner_name"),
        F.col("runner.movement.originBase").alias("origin_base"),
        F.col("runner.movement.start").alias("start_base"),
        F.col("runner.movement.end").alias("end_base"),
        F.col("runner.movement.outBase").alias("out_base"),
        F.col("runner.movement.isOut").alias("is_out"),
        F.col("runner.movement.outNumber").alias("out_number"),
        F.col("runner.details.event").alias("event"),
        F.col("runner.details.eventType").alias("event_type"),
        F.col("runner.details.responsiblePitcher.id").alias("responsible_pitcher_id"),
        F.col("runner.details.isScoringEvent").alias("is_scoring_event"),
        F.col("runner.details.rbi").alias("rbi"),
        F.col("runner.details.earned").alias("earned"),
        F.col("runner.details.teamUnearned").alias("team_unearned"),
        F.current_timestamp().alias("source_captured_at"),
    )

    return runners_extracted


# SCORING PLAYS
def _extract_scoring_plays(self, raw_df: DataFrame) -> DataFrame:
    """Extract plays that resulted in runs scored."""
    schema = StructType([
        StructField("gamePk", IntegerType(), True),
        StructField("liveData", StructType([
            StructField("plays", StructType([
                StructField("scoringPlays", ArrayType(IntegerType()), True),
                StructField("allPlays", ArrayType(StructType([
                    StructField("about", StructType([
                        StructField("atBatIndex", IntegerType(), True),
                        StructField("inning", IntegerType(), True),
                        StructField("halfInning", StringType(), True),
                    ]), True),
                    StructField("result", StructType([
                        StructField("event", StringType(), True),
                        StructField("description", StringType(), True),
                        StructField("rbi", IntegerType(), True),
                        StructField("awayScore", IntegerType(), True),
                        StructField("homeScore", IntegerType(), True),
                    ]), True),
                ])), True),
            ]), True),
        ]), True),
    ])

    parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

    # Get scoring play indices
    scoring_indices = parsed_df.select(
        F.col("parsed.gamePk").alias("game_pk"),
        F.explode(F.col("parsed.liveData.plays.scoringPlays")).alias("scoring_index")
    )

    # Get all plays
    all_plays = parsed_df.select(
        F.col("parsed.gamePk").alias("game_pk"),
        F.posexplode(F.col("parsed.liveData.plays.allPlays")).alias("idx", "play")
    )

    # Join to get only scoring plays
    scoring_plays = scoring_indices.join(
        all_plays,
        (scoring_indices.game_pk == all_plays.game_pk) &
        (scoring_indices.scoring_index == all_plays.idx)
    ).select(
        all_plays.game_pk,
        F.col("play.about.atBatIndex").alias("at_bat_index"),
        F.col("play.about.inning").alias("inning"),
        F.col("play.about.halfInning").alias("half_inning"),
        F.col("play.result.event").alias("event"),
        F.col("play.result.description").alias("description"),
        F.col("play.result.rbi").alias("rbi"),
        F.col("play.result.awayScore").alias("away_score"),
        F.col("play.result.homeScore").alias("home_score"),
        F.current_timestamp().alias("source_captured_at"),
    )

    return scoring_plays


# FIELDING CREDITS
def _extract_fielding_credits(self, raw_df: DataFrame) -> DataFrame:
    """Extract defensive credits (putouts, assists)."""
    schema = StructType([
        StructField("gamePk", IntegerType(), True),
        StructField("liveData", StructType([
            StructField("plays", StructType([
                StructField("allPlays", ArrayType(StructType([
                    StructField("about", StructType([
                        StructField("atBatIndex", IntegerType(), True),
                    ]), True),
                    StructField("runners", ArrayType(StructType([
                        StructField("credits", ArrayType(StructType([
                            StructField("player", StructType([
                                StructField("id", IntegerType(), True),
                            ]), True),
                            StructField("position", StructType([
                                StructField("code", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("abbreviation", StringType(), True),
                            ]), True),
                            StructField("credit", StringType(), True),
                        ])), True),
                    ])), True),
                ])), True),
            ]), True),
        ]), True),
    ])

    parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

    plays_df = parsed_df.select(
        F.col("parsed.gamePk").alias("game_pk"),
        F.explode(F.col("parsed.liveData.plays.allPlays")).alias("play")
    )

    runners_df = plays_df.select(
        F.col("game_pk"),
        F.col("play.about.atBatIndex").alias("at_bat_index"),
        F.explode(F.col("play.runners")).alias("runner")
    )

    credits_df = runners_df.select(
        F.col("game_pk"),
        F.col("at_bat_index"),
        F.explode(F.col("runner.credits")).alias("credit")
    )

    credits_extracted = credits_df.select(
        F.col("game_pk"),
        F.col("at_bat_index"),
        F.col("credit.player.id").alias("player_id"),
        F.col("credit.position.code").alias("position_code"),
        F.col("credit.position.name").alias("position_name"),
        F.col("credit.position.abbreviation").alias("position_abbrev"),
        F.col("credit.credit").alias("credit_type"),
        F.current_timestamp().alias("source_captured_at"),
    )

    return credits_extracted
