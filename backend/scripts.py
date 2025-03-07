import httpx
from .constants import BASE_URL
from .models import (
    Games,
    PlayByPlay,
    session,
    create_db_and_tables,
)
import tqdm
from sqlmodel import select, desc
from backend.data_collection import get_play_by_play_for_gameid, get_players_for_given_year
import click

def get_regular_season_games_to_db(year: int):
    create_db_and_tables()
    r = httpx.get(BASE_URL + f"schedule?sportId=1&season={year}&gameTypes=R")
    dates = r.json()["dates"]
    for date in tqdm.tqdm(dates):
        for game in date["games"]:
            game = Games(
                gameid=game["gamePk"],
                gameguid=game["gameGuid"],
                game_date=date["date"],
                away_team_id=game["teams"]["away"]["team"]["id"],
                home_team_id=game["teams"]["home"]["team"]["id"],
                game_type=game["gameType"],
            )
            session.add(game)
        session.commit()


def bulk_add_play_by_plays():
    """Loop through every game in the Games db and get the play by play data for each game"""
    games = session.exec(select(Games.gameid).order_by(desc(Games.game_date))).fetchall()
    got_games = set(
        session.exec(
            select(PlayByPlay.gameid)
            .distinct()
            .where((PlayByPlay.inning == 9) & (PlayByPlay.inning_half == "top"))
        ).fetchall()
    )
    for game in tqdm.tqdm(games):
        if game not in got_games:
            get_play_by_play_for_gameid(game)


def add_players_many_years():
    for yr in range(2010, 2026):
        get_players_for_given_year(yr)


@click.group()
def cli():
    pass

@cli.command()
@click.option("--start-year", type=int, help="The year to get regular season games for")
@click.option("--end-year", type=int, help="The year to get regular season games for")
def get_games(start_year, end_year):
    for year in range(start_year, end_year+1):
        get_regular_season_games_to_db(year)

@cli.command()
def bulk_add():
    bulk_add_play_by_plays()

if __name__ == "__main__":
    # create_db_and_tables()
    # get_regular_season_games_to_db(2024)
    # bulk_add_play_by_plays()
    cli()
