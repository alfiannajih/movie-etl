import sys
import os
import pathlib
import asyncio
from datetime import date, timedelta

sys.path.append(str(pathlib.Path(os.path.dirname(os.path.realpath(__file__)), "src")))

from prefect import flow
from src.movie_etl.flows.etl_flow import movies_flow

async def main(
    start_date: str=None,
    end_date: str=None,
    vote_count_minimum: int=50,
    # movie_limit: int=5,
    # person_limit: int=20,
    # review_limit: int=15
):
    if start_date == None and end_date == None:
        current_date = date.today()
        week_ago = (date.today() - timedelta(days=7))

        start_date = week_ago.strftime("%Y-%m-%d")
        end_date = current_date.strftime("%Y-%m-%d")
    
    await movies_flow(
        start_date=start_date,
        end_date=end_date,
        vote_count_minimum=vote_count_minimum,
        # movie_limit=movie_limit,
        # person_limit=person_limit,
        # review_limit=review_limit
    )

if __name__ == "__main__":
    asyncio.run(main())