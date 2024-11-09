import sys
import os
import pathlib
import asyncio

sys.path.append(str(pathlib.Path(os.path.dirname(os.path.realpath(__file__)), "src")))

from prefect import flow
from src.movie_etl.flows.etl_flow import movies_flow

@flow(
    name="Movies ETL Flow",
    log_prints=True
)
def main():
    asyncio.run(movies_flow())

if __name__ == "__main__":
    main.serve(
        name="Movies ETL Deployment",
    )