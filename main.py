import sys
import os
import pathlib
import asyncio
from datetime import date

sys.path.append(str(pathlib.Path(os.path.dirname(os.path.realpath(__file__)), "src")))

from prefect import flow, get_run_logger
from src.movie_etl.utils.etl import is_primary_key_exist_in_table, get_previous_week
from src.movie_etl.tasks.etl_task import get_movie_ids
from src.movie_etl.flows.etl_flow import single_movie_flow, engine

movie_limit = asyncio.Semaphore(3)

async def process_movie_with_semaphore(coro):
    async with movie_limit:
        return await coro

@flow(
    name="Movies ETL Flow",
    log_prints=True,
    flow_run_name="etl-flow-on-{start_date}--{end_date}"
)
async def movies_flow(
    start_date: date=get_previous_week(),
    end_date: date=date.today(),
    vote_count_minimum: int=1,
):
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    logger = get_run_logger()
    logger.info("Start movies ETL flow")
    movie_ids = await get_movie_ids(start_date=start_date, end_date=end_date, vote_count_minimum=vote_count_minimum)
    logger.info("Got " + str(len(movie_ids)) + " movie_ids")
    
    futures = []
    for movie_id in movie_ids:
        if is_primary_key_exist_in_table(movie_id, "movie_id", "movies", engine):
            logger.warning(f"Movie-{movie_id} already exist")
            continue
        futures.append(process_movie_with_semaphore(single_movie_flow(movie_id)))
    await asyncio.gather(*futures)

    logger.info("Finished movies ETL flow")

if __name__ == "__main__":
    asyncio.run(movies_flow())