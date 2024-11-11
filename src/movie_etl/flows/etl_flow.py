import requests
from dotenv import load_dotenv
import os
from typing import List, Dict
from sqlalchemy import create_engine
import asyncio
from prefect import task, get_run_logger, flow
from prefect.context import FlowRunContext

from src.movie_etl.utils.etl import is_primary_key_exist_in_table
from src.movie_etl.tasks.etl_task import (
    get_movie_ids,
    get_data_from_tmdb_api,
    scrape_data_from_imdb,
    clean_movie_details,
    clean_collection_details,
    clean_company_details,
    clean_person_details,
    clean_imdb_reviews,
    clean_imdb_user_details,
    load_data_to_db
)

load_dotenv()

engine = create_engine(os.getenv("DB_CONNECTION"))

people_details_limit = asyncio.Semaphore(15)
movie_limit = asyncio.Semaphore(5)
review_limit = asyncio.Semaphore(10)

async def process_people_with_semaphore(coro):
    async with people_details_limit:
        return await coro
    
async def process_movie_with_semaphore(coro):
    async with movie_limit:
        return await coro
    
async def process_review_with_semaphore(coro):
    async with review_limit:
        return await coro

@flow(
    name="movie_details_flow",
    log_prints=True,
    retries=2,
    flow_run_name="movie-details-flow-on-{movie_id}"
)
async def movie_details_flow(
    movie_id: int,
):
    logger = get_run_logger()
    movie_details = await get_data_from_tmdb_api(
        id=movie_id,
        url="https://api.themoviedb.org/3/movie",
        endpoint="movie",
        params={
            "append_to_response": "credits"
        }
    )
    movie_details = await clean_movie_details(movie_details["id"], movie_details)

    if movie_details["collection_id"] != None:
        logger.info("Collection exists for movie_id: " + str(movie_id))
    
        await movie_collection_flow(movie_details["collection_id"])
    
    # load_movie_details_to_db(movie_id, movie_details)
    await load_data_to_db(
        table_name="movies",
        data={k: movie_details[k] for k in [
            "movie_id",
            "collection_id",
            "imdb_id",
            "title",
            "overview",
            "release_date",
            "popularity",
            "vote_average",
            "vote_count",
            "budget",
            "revenue",
            "runtime"
        ]},
        id=movie_id,
        engine=engine
    )

    return movie_details

@flow(
    name="movie_collection_flow",
    log_prints=True,
    retries=2,
    flow_run_name="movie-collection-flow-on-{collection_id}"
)
async def movie_collection_flow(
    collection_id: int,
):
    if not is_primary_key_exist_in_table(collection_id, "collection_id", "movie_collections", engine):
        collection_details = await get_data_from_tmdb_api(
            id=collection_id,
            url="https://api.themoviedb.org/3/collection",
            endpoint="collection"
        )
        collection_details = await clean_collection_details(collection_id, collection_details)
        # load_collection_details_to_db(collection_id, collection_details)
        await load_data_to_db(
            table_name="movie_collections",
            data=collection_details,
            id=collection_id,
            engine=engine
        )

@flow(
    name="movie_genre_flow",
    log_prints=True,
    retries=2,
    flow_run_name="movie-genre-flow-on-{movie_id}"
)
async def movie_genre_flow(
    movie_id: int,
    movie_genres: List
):
    logger = get_run_logger()
    
    for genre in movie_genres:
        if not is_primary_key_exist_in_table(genre["genre_id"], "genre_id", "genres", engine):
            logger.info("New genre ID found: " + str(genre["genre_id"]))
            # load_genre_to_db(genre["id"], genre)
            await load_data_to_db(
                table_name="genres",
                data=genre,
                id=genre["genre_id"],
                engine=engine
            )

        # load_movie_genre_to_db(movie_id, {"genre_id": genre["id"], "movie_id": movie_id})
        await load_data_to_db(
            table_name="movie_genre",
            data={"genre_id": genre["genre_id"], "movie_id": movie_id},
            id=movie_id,
            engine=engine
        )

@flow(
    name="movie_language_flow",
    log_prints=True,
    retries=2,
    flow_run_name="movie-language-flow-on-{movie_id}"
)
async def movie_language_flow(
    movie_id: int,
    movie_languages: List
):
    for language_id in movie_languages:
        # load_language_to_db(movie_id, {"language_id": language_id, "movie_id": movie_id})
        await load_data_to_db(
            table_name="movie_language",
            data={"language_id": language_id, "movie_id": movie_id},
            id=movie_id,
            engine=engine
        )

@flow(
    name="movie_production_flow",
    log_prints=True,
    retries=2,
    flow_run_name="movie-production-flow-on-{movie_id}"
)
async def movie_production_flow(
    movie_id: int,
    movie_productions: List
):
    logger = get_run_logger()
    for company_id in movie_productions:
        companies_to_add = []
        if not is_primary_key_exist_in_table(company_id, "company_id", "companies", engine):
            company_details = await get_data_from_tmdb_api(
                id=company_id,
                url="https://api.themoviedb.org/3/company",
                endpoint="company"
            )
            company_details = await clean_company_details(company_id, company_details)
            companies_to_add.append(company_details)
            parent_company_id = company_details["parent_company_id"]

            while parent_company_id != None:
                if not is_primary_key_exist_in_table(parent_company_id, "company_id", "companies", engine):
                    parent_company_details = await get_data_from_tmdb_api(
                        id=parent_company_id,
                        url="https://api.themoviedb.org/3/company",
                        endpoint="company"
                    )
                    parent_company_details = await clean_company_details(parent_company_id, parent_company_details)
                    companies_to_add.append(parent_company_details)
                
                companies_to_add.append(parent_company_details)
                parent_company_id = companies_to_add[-1]["parent_company_id"]

            for i in range(len(companies_to_add)-1, -1, -1):
                # load_company_to_db(companies_to_add[i]["company_id"], companies_to_add[i])
                await load_data_to_db(
                    table_name="companies",
                    data=companies_to_add[i],
                    id=companies_to_add[i]["company_id"],
                    engine=engine
                )
            
        # load_movie_production_to_db(movie_id, {"company_id": company_id, "movie_id": movie_id})
        await load_data_to_db(
            table_name="movie_production",
            data={"company_id": company_id, "movie_id": movie_id},
            id=movie_id,
            engine=engine
        )

@flow(
    name="movie_reviews_flow",
    log_prints=True,
    retries=2,
    flow_run_name="movie-reviews-flow-on-{movie_id}"
)
async def movie_reviews_flow(
    imdb_movie_id: str,
    movie_id: int,
) -> Dict:
    soup = await scrape_data_from_imdb(
        imdb_movie_id,
        "https://www.imdb.com/title",
        "reviews/_ajax"
    )

    cleaned_reviews = await clean_imdb_reviews(movie_id, soup)
    
    futures = [process_review_with_semaphore(imdb_user_reviews_flow(review["movie_id"], review)) for review in cleaned_reviews]
    await asyncio.gather(*futures)

@flow(
    name="imdb_user_details_flow",
    log_prints=True,
    retries=2,
    flow_run_name="imdb-user-details-flow-on-{user_id}"
)
async def imdb_user_details_flow(
    user_id: str,
):
    soup = await scrape_data_from_imdb(
        url="https://www.imdb.com/user",
        imdb_id=user_id
    )

    user_details = await clean_imdb_user_details(user_id, soup)
    await load_data_to_db(
        table_name="imdb_users",
        id=user_id,
        data={k: user_details[k] for k in [
            "user_id",
            "user_name",
            "date_joined"
        ]},
        engine=engine
    )

@flow(
    name="imdb_user_reviews_flow",
    log_prints=True,
    retries=2,
    flow_run_name="imdb-user-reviews-flow-on-{movie_id}"
)
async def imdb_user_reviews_flow(
    movie_id: int,
    review: dict
):
    if not is_primary_key_exist_in_table(review["user_id"], "user_id", "imdb_users", engine):
        await imdb_user_details_flow(review["user_id"])
    
    await load_data_to_db(
        table_name="imdb_movie_reviews",
        id=review["review_id"],
        data=review,
        engine=engine
    )

async def single_movie_cast_flow(
    movie_id: int,
    cast: dict
):
    if not is_primary_key_exist_in_table(cast["person_id"], "person_id", "people", engine):
        person_details = await get_data_from_tmdb_api(
            id=cast["person_id"],
            url="https://api.themoviedb.org/3/person",
            endpoint="person"
        )
        person_details = await clean_person_details(person_details["id"], person_details)

        # load_person_to_db(cast["person_id"], person_details)
        await load_data_to_db(
            table_name="people",
            data=person_details,
            id=person_details["person_id"],
            engine=engine
        )
    
    # load_movie_cast_to_db(movie_id, cast | {"movie_id": movie_id})
    await load_data_to_db(
        table_name="movie_cast",
        data=cast | {"movie_id": movie_id},
        id=movie_id,
        engine=engine
    )

@flow(
    name="movie_cast_flow",
    log_prints=True,
    retries=2,
    flow_run_name="movie-cast-flow-on-{movie_id}"
)
async def movie_cast_flow(
    movie_id: int,
    movie_casts: List
):
    logger = get_run_logger()
    futures = [process_people_with_semaphore(single_movie_cast_flow(movie_id, cast)) for cast in movie_casts]
    await asyncio.gather(*futures)

async def single_movie_crew_flow(
    movie_id: int,
    crew: dict
):
    if not is_primary_key_exist_in_table(crew["person_id"], "person_id", "people", engine):
        person_details = await get_data_from_tmdb_api(
            id=crew["person_id"],
            url="https://api.themoviedb.org/3/person",
            endpoint="person"
        )
        person_details = await clean_person_details(person_details["id"], person_details)

        # load_person_to_db(crew["person_id"], person_details)
        await load_data_to_db(
            table_name="people",
            data=person_details,
            id=person_details["person_id"],
            engine=engine
        )

    # load_movie_crew_to_db(movie_id, crew | {"movie_id": movie_id})
    await load_data_to_db(
        table_name="movie_crew",
        data=crew | {"movie_id": movie_id},
        id=movie_id,
        engine=engine
    )

@flow(
    name="movie_crew_flow",
    log_prints=True,
    retries=2,
    flow_run_name="movie-crew-flow-on-{movie_id}"
)
async def movie_crew_flow(
    movie_id: int,
    movie_crews: List
):
    logger = get_run_logger()
    futures = [process_people_with_semaphore(single_movie_crew_flow(movie_id, crew)) for crew in movie_crews]
    await asyncio.gather(*futures)

@flow(
    name="single_movie_flow",
    log_prints=True,
    retries=2,
    flow_run_name="etl-flow-on-{movie_id}"
)
async def single_movie_flow(movie_id: int):
    movie_details = await movie_details_flow(movie_id)
    # reviews = await movie_reviews_flow(
    #     movie_details["imdb_id"],
    #     movie_id
    # )

    futures = [
        movie_genre_flow(movie_id, movie_details["genres"]),
        movie_language_flow(movie_id, movie_details["spoken_languages"]),
        movie_production_flow(movie_id, movie_details["production_companies"]),
        movie_cast_flow(movie_id, movie_details["casts"]),
        movie_crew_flow(movie_id, movie_details["crews"]),
        movie_reviews_flow(movie_details["imdb_id"], movie_id)
    ]
    # futures.extend([imdb_user_reviews_flow(review["user_id"], review) for review in reviews])
    await asyncio.gather(*futures)

@flow(
    name="Movies ETL Flow",
    log_prints=True,
    retries=2,
    flow_run_name="etl-flow-on-{start_date}--{end_date}"
)
async def movies_flow(
    start_date: str="2024-10-15",
    end_date: str="2024-11-01",
    vote_count_minimum: int=2,
):
    logger = get_run_logger()
    logger.info("Start movies ETL flow")
    movie_ids = await get_movie_ids(start_date=start_date, end_date=end_date, vote_count_minimum=vote_count_minimum)
    logger.info("Got " + str(len(movie_ids)) + " movie_ids")

    # for movie_id in movie_ids:
    #     logger.info("Start processing movie_id: " + str(movie_id))

    #     if is_primary_key_exist_in_table(movie_id, "movie_id", "movies", engine):
    #         logger.warning("Movie details already exist")
    #         continue

    #     single_movie_flow(movie_id)
    
    task_runner_type = type(FlowRunContext.get().task_runner)
    futures = []
    for movie_id in movie_ids:
        if is_primary_key_exist_in_table(movie_id, "movie_id", "movies", engine):
            logger.warning("Movie details already exist")
            continue
        futures.append(process_movie_with_semaphore(single_movie_flow.with_options(task_runner=task_runner_type())(movie_id)))
    await asyncio.gather(*futures)

    logger.info("Finished movies ETL flow")