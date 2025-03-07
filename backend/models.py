import httpx
from sqlmodel import Field, Session, SQLModel, create_engine, JSON
from backend.constants import database
from typing import Any

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

engine = create_engine(database)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


session = Session(engine)


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


def encode_base_state(
    runner_on_first: bool, runner_on_second: bool, runner_on_third: bool
) -> int:
    return int(runner_on_first) + 2 * int(runner_on_second) + 4 * int(runner_on_third)


class Games(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    gameid: int
    gameguid: str
    game_date: str
    away_team_id: int
    home_team_id: int
    game_type: str


class InningsFinal(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    gameid: int
    inning: int
    inning_half: str
    runs_scored: int


class PlayByPlay(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    gameid: int
    inning: int
    inning_half: str
    batter: int
    pitcher: int
    runner_on_first: bool
    runner_on_second: bool
    runner_on_third: bool

    runner_on_first_after: bool
    runner_on_second_after: bool
    runner_on_third_after: bool
    outs: int
    runs_scored: int
    runs_scored_before: int
    result: str

    # Add these computed fields
    base_state_before: int = 0
    base_state_after: int = 0
    # For sorting
    play_end_time: str
    ab_index: int


class Players(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    playerid: int
    name: str
    draft_year: int | None
    mlb_debut_date: str | None
    years_positions: dict[str, Any] = Field(default={}, sa_type=JSON)

    def get_position_for_year(self, year: int) -> dict[str, Any]:
        """Get position information for a specific year"""
        return self.years_positions.get(str(year), {})

    def update_position(self, year: int, position_code: str) -> None:
        """Update position information for a specific year"""
        # Create a new dictionary if years_positions is None
        new_positions = {}

        # Copy existing entries if any
        if self.years_positions:
            new_positions.update(self.years_positions)

        # Add the new year/position
        new_positions[str(year)] = position_code

        # Assign the NEW dictionary (this triggers SQLAlchemy to track the change)
        self.years_positions = new_positions


class YearlyRE24(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    year:int
    base_state: int
    outs: int
    expected_runs: float
    count: int
    base_state_description: str

class BatterRunValue(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    year: int
    playerid: int
    name: str
    plate_appearances: int
    total_run_value: float

class PitcherRunValue(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    year: int
    playerid: int
    name: str
    plate_appearances:int
    total_run_value: float
