import httpx
import hishel
from .constants import BASE_URL
import time
from sqlmodel import select
from .models import PlayByPlay, InningsFinal, session, encode_base_state, Players
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


@retry(
    stop=stop_after_attempt(3),
    reraise=True,
    retry=retry_if_exception_type((httpx.ConnectError, httpx.TimeoutException)),
    wait=wait_exponential(multiplier=3, min=1, max=30),
)
def get_response(url: str, client):
    with client as c:
        response = c.get(url, timeout=5)
        return response


def get_play_by_play_for_gameid(gameid: str | int) -> bool:
    url = BASE_URL + f"/game/{gameid}/playByPlay"
    max_retries = 1
    retries_left = max_retries + 1
    response = None
    while retries_left > 0:
        try:
            client = hishel.CacheClient(storage=hishel.SQLiteStorage())
            response = get_response(url, client)
            break  # Success! Break out of the loop
        except RuntimeError:
            retries_left -= 1
            if retries_left > 0:
                print(
                    f"Request failed, retrying in 5 seconds. {retries_left} attempts remaining."
                )
                time.sleep(5)
            else:
                print("All retry attempts exhausted.")
                raise ValueError("Failed to get response after multiple attempts")

    if not response or response.status_code != 200:
        raise ValueError("Bad response")

    all_plays = response.json()["allPlays"]
    scored = 0
    rolling_inning = "top1"
    runner_on_first_after = False
    runner_on_second_after = False
    runner_on_third_after = False
    try:
        for play in all_plays:
            matchup = play.get("matchup", {})
            about = play.get("about", {})
            current_inning = about["inning"]
            current_half = about["halfInning"]
            if f"{current_half}{current_inning}" != rolling_inning:
                rolling_inning = f"{current_half}{current_inning}"
                runner_on_first_after = False
                runner_on_second_after = False
                runner_on_third_after = False
                scored = 0
            runners = play.get("runners", [])
            runner_on_first = runner_on_first_after
            runner_on_second = runner_on_second_after
            runner_on_third = runner_on_third_after
            runner_on_first_after = "postOnFirst" in matchup
            runner_on_second_after = "postOnSecond" in matchup
            runner_on_third_after = "postOnThird" in matchup
            scored_on_play = 0
            for runner in runners:
                end = runner["movement"]["end"]
                if end == "score":
                    scored += 1
                    scored_on_play += 1

            pp = PlayByPlay(
                gameid=gameid,
                inning=current_inning,
                inning_half=current_half,
                batter=matchup["batter"]["id"],
                pitcher=matchup["pitcher"]["id"],
                runner_on_first=runner_on_first,
                runner_on_second=runner_on_second,
                runner_on_third=runner_on_third,
                runner_on_first_after=runner_on_first_after,
                runner_on_second_after=runner_on_second_after,
                runner_on_third_after=runner_on_third_after,
                outs=play["count"]["outs"],
                runs_scored_before=scored - scored_on_play,
                runs_scored=scored_on_play,
                result=play["result"].get("event", "n/a").lower().replace(" ", "_"),
                base_state_before=encode_base_state(
                    runner_on_first, runner_on_second, runner_on_third
                ),
                base_state_after=encode_base_state(
                    runner_on_first_after, runner_on_second_after, runner_on_third_after
                ),
                play_end_time=about["endTime"],
                ab_index=about["atBatIndex"],
            )
            session.add(pp)

            if play["count"]["outs"] == 3:
                inning_final = InningsFinal(
                    gameid=gameid,
                    inning=current_inning,
                    inning_half=current_half,
                    runs_scored=scored,
                )
                session.add(inning_final)

        session.commit()
    except Exception as e:
        print(e, gameid)
        session.rollback()
        return False


def get_players_for_given_year(year: int):
    """Fetch players from the MLB API for a given year and update their positions"""
    r = httpx.get(BASE_URL + f"sports/1/players?season={year}")
    players = r.json()["people"]

    for player_data in players:
        player_id = player_data["id"]

        # Check if player exists in database
        db_player = session.exec(
            select(Players).where(Players.playerid == player_id)
        ).first()

        if not db_player:
            # Create new player
            db_player = Players(
                playerid=player_id,
                name=player_data.get("fullName", ""),
                draft_year=player_data.get("draftYear"),
                mlb_debut_date=player_data.get("mlbDebutDate"),
            )
        # Update position data for this year
        if "primaryPosition" in player_data:
            db_player.update_position(year, player_data["primaryPosition"]["code"])

        session.add(db_player)

    session.commit()
