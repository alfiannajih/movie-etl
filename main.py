import sys
import os
import pathlib
import asyncio
from datetime import date

sys.path.append(str(pathlib.Path(os.path.dirname(os.path.realpath(__file__)), "src")))

from prefect import flow, get_run_logger
from src.movie_etl.utils.etl import is_primary_key_exist_in_table, get_previous_week, generate_flow_run_name
from src.movie_etl.tasks.etl_task import get_movie_ids
from src.movie_etl.flows.etl_flow import single_movie_flow, engine

@flow(
    name="Movies ETL Flow",
    log_prints=True,
    flow_run_name=generate_flow_run_name,
    validate_parameters=False
)
async def movies_flow(
    start_date: date=None,
    end_date: date=None,
    vote_count_minimum: int=5,
    movie_limit: int=3,
    person_limit: int=10
):
    if start_date is None or end_date is None:
        end_date = date.today()
        start_date = get_previous_week()
    
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    limit = asyncio.Semaphore(movie_limit)
    async def process_movie_with_semaphore(coro):
        async with limit:
            return await coro

    logger = get_run_logger()
    logger.info("Start movies ETL flow")
    movie_ids = await get_movie_ids(start_date=start_date, end_date=end_date, vote_count_minimum=vote_count_minimum)
    logger.info("Got " + str(len(movie_ids)) + " movie_ids")
    
    futures = []
    for movie_id in movie_ids:
        if is_primary_key_exist_in_table(movie_id, "movie_id", "movies", engine):
            logger.warning(f"Movie-{movie_id} already exist")
            
        futures.append(process_movie_with_semaphore(single_movie_flow(movie_id, person_limit)))
    await asyncio.gather(*futures)

    logger.info("Finished movies ETL flow")

if __name__ == "__main__":
    asyncio.run(movies_flow())