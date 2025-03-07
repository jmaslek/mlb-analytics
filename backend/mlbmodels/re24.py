"""Run Expectancy For The 24 Base-Out States"""

from backend.constants import database, decode_base_state
from sqlmodel import create_engine, select, Session
import polars as pl
from functools import lru_cache
from contextlib import contextmanager
from ..models import BatterRunValue, PitcherRunValue, YearlyRE24
conn = create_engine(database)


@contextmanager
def get_db_session():
    session = Session(conn)
    try:
        yield session
    finally:
        session.close()

@lru_cache
def get_players():
    return pl.read_database("SELECT * FROM players", conn.connect())

@lru_cache
def get_year_query_db(year:int):
    query = f"""
        SELECT
            p.gameid, p.inning, p.inning_half, p.outs, p.batter, p.pitcher, p.runs_scored, p.runs_scored_before, p.result, p.base_state_before, i.runs_scored as runs_scored_final, g.game_date, p.ab_index, p.base_state_after
        FROM
            PlayByPlay p
        JOIN
            InningsFinal i
        ON
            p.gameid = i.gameid and
            p.inning = i.inning and
            p.inning_half = i.inning_half
        JOIN
            games g
        ON
            g.gameid = p.gameid
        WHERE substring(g.game_date, 1, 4) = '{year}' 
        """
    df = pl.read_database(query, conn.connect()).unique()
    df = (
        df.with_columns(
            (pl.col("runs_scored_final") - pl.col("runs_scored_before")).alias("runs_after")
        )
        .sort(
            ["gameid", "inning", "inning_half", "ab_index"],
            descending=[False, False, True, False],
        )
        .with_columns(
            [
                pl.col("outs")
                .shift(1)
                .over(["gameid", "inning", "inning_half", "game_date"])
                .alias("previous_outs")
                .fill_null(0)
            ]
        )
    )
    return df


@lru_cache
def get_re24_specific_year(year:int):
    df = get_year_query_db(year)
    # Add a column for runs scored from the AB to the end of the inning and then how many outs
    # there were before the AB

    re = (
        df.group_by(["base_state_before", "previous_outs"])
        .agg(
            [
                pl.col("runs_after").mean().round(3).alias("expected_runs"),
                pl.len().alias("count"),
            ]
        )
        .sort(["base_state_before", "previous_outs"])
    )
    re = re.with_columns(
        pl.col("base_state_before")
        .map_elements(decode_base_state, return_dtype=str)
        .alias("base_state_description")
    )
    return re

def calculate_batters_run_value(year:int, min_ab:int=50):
    df = get_year_query_db(year)
    players = get_players().select(['playerid', 'name'])
    re_year = get_re24_specific_year(year)
    re_lookup = {}
    for row in re_year.iter_rows(named=True):
        re_lookup[(row["base_state_before"], row["previous_outs"])] = row["expected_runs"]
    # %%
    # Back to df.  We want to add a column for the RE of each entry

    df = df.with_columns(
        pl.struct(pl.col("base_state_before"), pl.col("previous_outs"))
        .map_elements(
            lambda row: re_lookup[(row["base_state_before"], row["previous_outs"])],
            return_dtype=float,
        )
        .alias("re_ab_start")
    )

    # Function to calculate RE afterwards
    def re_after(row):
        if row["outs"] == 3:
            return 0
        return re_lookup[(row["base_state_after"], row["outs"])]

    df = df.with_columns(
        pl.struct(pl.col("base_state_after"), pl.col("outs"))
        .map_elements(re_after, return_dtype=float)
        .alias("re_ab_end")
    )

    df = df.with_columns(
        (pl.col("runs_scored") + pl.col("re_ab_end") - pl.col("re_ab_start")).alias(
            "run_value_added"
        )
    )
    player_stats = (
        df.group_by("batter")
        .agg(
            [
                pl.len().alias("plate_appearances"),
                pl.col("run_value_added").sum().round(2).alias("total_run_value"),
            ]
        )
        .sort("total_run_value", descending=True)
    )
    player_stats = player_stats.join(players,
                                     left_on='batter',
                                     right_on='playerid').sort('total_run_value', descending=True)
    # Insert into DB:
    with get_db_session() as stats_session:
        to_insert = player_stats.to_dicts()
        for entry in to_insert:
            stats_session.add(
                BatterRunValue(
                year = year,
                    playerid = entry['batter'],
                    plate_appearances=entry['plate_appearances'],
                    total_run_value=entry['total_run_value'],
                    name = entry['name']
                )
            )
        stats_session.commit()

    # Sample size
    player_stats = player_stats.filter(pl.col("plate_appearances") >= min_ab)
    return player_stats

def get_batters_run_value(year:int, min_ab:int=50):
    with get_db_session() as sesh:
        years = sesh.exec(select(
            BatterRunValue.year
        ).distinct()
        ).fetchall()

    if year not in years:
        return calculate_batters_run_value(year, min_ab)
    else:
        with get_db_session() as sesh:
            data = sesh.exec(
                select(
                    BatterRunValue
                ).where(
                    BatterRunValue.year == year
                ).where(
                    BatterRunValue.plate_appearances >= min_ab
                )
            ).fetchall()
        return pl.DataFrame([d.model_dump() for d in data])




def calculate_pitchers_run_value(year:int, min_ab:int=50):
    df = get_year_query_db(year)
    players = get_players().select(['playerid', 'name'])
    re_year = get_re24_specific_year(year)
    re_lookup = {}
    for row in re_year.iter_rows(named=True):
        re_lookup[(row["base_state_before"], row["previous_outs"])] = row["expected_runs"]
    # %%
    # Back to df.  We want to add a column for the RE of each entry

    df = df.with_columns(
        pl.struct(pl.col("base_state_before"), pl.col("previous_outs"))
        .map_elements(
            lambda row: re_lookup[(row["base_state_before"], row["previous_outs"])],
            return_dtype=float,
        )
        .alias("re_ab_start")
    )

    # Function to calculate RE afterwards
    def re_after(row):
        if row["outs"] == 3:
            return 0
        return re_lookup[(row["base_state_after"], row["outs"])]

    df = df.with_columns(
        pl.struct(pl.col("base_state_after"), pl.col("outs"))
        .map_elements(re_after, return_dtype=float)
        .alias("re_ab_end")
    )

    df = df.with_columns(
        (pl.col("runs_scored") + pl.col("re_ab_end") - pl.col("re_ab_start")).alias(
            "run_value_added"
        )
    )
    player_stats = (
        df.group_by("pitcher")
        .agg(
            [
                pl.len().alias("plate_appearances"),
                pl.col("run_value_added").sum().round(2).alias("total_run_value"),
            ]
        )
        .sort("total_run_value", descending=True)  # Minimum sample size
    )

    player_stats = player_stats.join(players,
                                     left_on='pitcher',
                                     right_on='playerid').sort('total_run_value', descending=True)
    # Insert into DB:
    with get_db_session() as stats_session:
        to_insert = player_stats.to_dicts()
        for entry in to_insert:
            stats_session.add(
                PitcherRunValue(
                    year=year,
                    playerid=entry["pitcher"],
                    plate_appearances=entry["plate_appearances"],
                    total_run_value=entry["total_run_value"],
                    name=entry["name"],
                )
            )
        stats_session.commit()

    player_stats = player_stats.filter(pl.col("plate_appearances") >= min_ab)
    return player_stats[::-1]

def get_pitchers_run_value(year:int, min_ab:int=50):
    with get_db_session() as sesh:
        years = sesh.exec(select(
            PitcherRunValue.year
        ).distinct()
        ).fetchall()

    if year not in years:
        return calculate_pitchers_run_value(year, min_ab)
    else:
        with get_db_session() as sesh:
            data = sesh.exec(
                select(
                    PitcherRunValue
                ).where(
                    PitcherRunValue.year == year
                ).where(
                    PitcherRunValue.plate_appearances >= min_ab
                )
            ).fetchall()
        return pl.DataFrame([d.model_dump() for d in data])
