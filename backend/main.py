from fastapi import FastAPI
from .mlbmodels.re24 import *
app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/re24/{year}")
async def re24(year: int):
    return get_re24_specific_year(year).to_dicts()


@app.get("/run-value/batters/{year}")
async def run_value_batters(year: int):
    return get_batters_run_value(year).to_dicts()

@app.get("/run-value/pitchers/{year}")
async def run_value_batters(year: int):
    return get_pitchers_run_value(year).to_dicts()