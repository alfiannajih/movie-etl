from dotenv import load_dotenv
import os
from typing import List, Dict
from sqlalchemy import create_engine, URL
import asyncio
from prefect import get_run_logger, flow

from src.movie_etl.utils.etl import is_primary_key_exist_in_table, rollback_movie
from src.movie_etl.tasks.etl_task import (
    get_data_from_tmdb_api,
    clean_movie_details,
    clean_collection_details,
    clean_company_details,
    clean_person_details,
    clean_watch_providers,
    clean_genres,
    clean_languages,
    clean_production_countries,
    load_single_row_to_db,
    load_multi_row_to_db,
    scrape_html_content,
    clean_imdb_ratings,
    clean_rotten_tomatoes_ratings,
    clean_metacritic_ratings,
    clean_wikidata
)

load_dotenv()

url_object = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    database=os.getenv("POSTGRES_DB"),
    port=os.getenv("POSTGRES_PORT")
)

engine = create_engine(url_object)

@flow(
    name="Movie Details ETL",
    log_prints=True,
    flow_run_name="movie-details-flow-on-{movie_id}"
)
async def movie_details_flow(
    movie_id: int,
):
    logger = get_run_logger()
    movie_details = await get_data_from_tmdb_api(
        id=movie_id,
        url="https://api.themoviedb.org/3/movie",
        endpoint_name="movie",
        params={
            "append_to_response": "credits,watch/providers,external_ids"
        }
    )
    movie_details = await clean_movie_details(movie_details["id"], movie_details)

    if movie_details["collection_id"] != None:
        logger.info("Collection exists for movie_id: " + str(movie_id))
    
        await movie_collection_flow(movie_details["collection_id"])
    
    await load_single_row_to_db(
        table_name="movies",
        data={k: movie_details[k] for k in [
            "movie_id",
            "collection_id",
            "title",
            "overview",
            "release_date",
            "popularity",
            "budget",
            "revenue",
            "runtime"
        ]},
        primary_key_id=movie_id,
        engine=engine
    )

    return movie_details

@flow(
    name="Collection Details ETL",
    log_prints=True,
    flow_run_name="movie-collection-flow-on-{collection_id}"
)
async def movie_collection_flow(
    collection_id: int,
):
    if not is_primary_key_exist_in_table(collection_id, "collection_id", "movie_collections", engine):
        collection_details = await get_data_from_tmdb_api(
            id=collection_id,
            url="https://api.themoviedb.org/3/collection",
            endpoint_name="collection"
        )
        collection_details = await clean_collection_details(collection_id, collection_details)
        await load_single_row_to_db(
            table_name="movie_collections",
            data=collection_details,
            primary_key_id=collection_id,
            engine=engine
        )

@flow(
    name="Movie Genre Load",
    log_prints=True,
    flow_run_name="movie-genre-flow-on-{movie_id}"
)
async def movie_genre_flow(
    movie_id: int,
    movie_genres: List
):  
    genres = await clean_genres(movie_genres, movie_id)

    await load_multi_row_to_db(
        table_name="movie_genre",
        columns=["movie_id", "genre_id"],
        data=genres,
        engine=engine
    )

@flow(
    name="Movie Language Load",
    log_prints=True,
    flow_run_name="movie-language-flow-on-{movie_id}"
)
async def movie_language_flow(
    movie_id: int,
    movie_languages: List
):
    languages = await clean_languages(movie_languages, movie_id)

    await load_multi_row_to_db(
        table_name="movie_language",
        columns=["movie_id", "language_id"],
        data=languages,
        engine=engine
    )

@flow(
    name="Company Details ET",
    log_prints=True,
    flow_run_name="company-flow-on-{company_id}"
)
async def company_details_flow(
    company_id: int,
) -> Dict:
    company_details = await get_data_from_tmdb_api(
        id=company_id,
        url="https://api.themoviedb.org/3/company",
        endpoint_name="company"
    )
    company_details = await clean_company_details(company_id, company_details)

    return company_details

@flow(
    name="Movie Production ETL",
    log_prints=True,
    flow_run_name="movie-production-flow-on-{movie_id}"
)
async def movie_production_flow(
    movie_id: int,
    movie_productions: List
):
    for company_id in movie_productions:
        companies_to_add = []
        if not is_primary_key_exist_in_table(company_id, "company_id", "companies", engine):
            company_details = await company_details_flow(company_id)
            companies_to_add.append(company_details)
            parent_company_id = company_details["parent_company_id"]

            while parent_company_id != None:
                if not is_primary_key_exist_in_table(parent_company_id, "company_id", "companies", engine):
                    parent_company_details = await company_details_flow(parent_company_id)
                    companies_to_add.append(parent_company_details)
                
                companies_to_add.append(parent_company_details)
                parent_company_id = companies_to_add[-1]["parent_company_id"]

            for i in range(len(companies_to_add)-1, -1, -1):
                await load_single_row_to_db(
                    table_name="companies",
                    data=companies_to_add[i],
                    primary_key_id=companies_to_add[i]["company_id"],
                    engine=engine
                )
            
        await load_single_row_to_db(
            table_name="movie_production",
            data={"company_id": company_id, "movie_id": movie_id},
            primary_key_id=company_id,
            engine=engine
        )

@flow(
    name="Movie Production Countries Load",
    log_prints=True,
    retries=1,
    flow_run_name="movie-production-country-flow-on-{movie_id}"
)
async def movie_production_country_flow(
    movie_id: int,
    production_countries: List
):
    countries = await clean_production_countries(production_countries, movie_id)

    await load_multi_row_to_db(
        table_name="production_country",
        columns=["movie_id", "country_id"],
        data=countries,
        engine=engine
    )

@flow(
    name="Movie Provider ETL",
    log_prints=True,
    flow_run_name="movie-provider-flow-on-{movie_id}"
)
async def movie_provder_flow(
    movie_id: int,
    movie_providers: Dict
):
    logger = get_run_logger()

    add_to_db = await clean_watch_providers(movie_id, movie_providers)
    
    if len(add_to_db) > 0:
        await load_multi_row_to_db(
            table_name="movie_provider",
            columns=["movie_id", "country_id", "provider_id", "type"],
            data=add_to_db,
            engine=engine
        )
    else:
        logger.warning("Watch providers doesn't exists")

@flow(
    name="Cast Flow ETL",
    log_prints=True,
    flow_run_name="cast-flow-of-{person_id}-on-{movie_id}"
)
async def cast_flow(
    movie_id: int,
    cast: Dict,
    person_id: int
):
    logger = get_run_logger()
    if not is_primary_key_exist_in_table(person_id, "person_id", "people", engine):
        logger.warning(f"Person with primary id of {person_id} doesn't exists!")
        person_details = await get_data_from_tmdb_api(
            id=cast["person_id"],
            url="https://api.themoviedb.org/3/person",
            endpoint_name="person"
        )
        person_details = await clean_person_details(person_details["id"], person_details)

        await load_single_row_to_db(
            table_name="people",
            data=person_details,
            primary_key_id=person_details["person_id"],
            engine=engine
        )
    
    await load_single_row_to_db(
        table_name="movie_cast",
        data=cast | {"movie_id": movie_id},
        primary_key_id=person_id,
        engine=engine
    )

@flow(
    name="Parallel Movie Cast Flow",
    log_prints=True,
    flow_run_name="movie-cast-flow-on-{movie_id}"
)
async def movie_cast_flow(
    movie_id: int,
    movie_casts: List
):
    cast_details_limit = asyncio.Semaphore(10)

    async def process_cast_with_semaphore(coro):
        async with cast_details_limit:
            return await coro

    futures = [process_cast_with_semaphore(cast_flow(movie_id, cast, cast["person_id"])) for cast in movie_casts]
    await asyncio.gather(*futures)

@flow(
    name="Crew Flow ETL",
    log_prints=True,
    flow_run_name="crew-flow-of-{person_id}-on-{movie_id}"
)
async def crew_flow(
    movie_id: int,
    crew: Dict,
    person_id: int
):
    logger = get_run_logger()
    if not is_primary_key_exist_in_table(person_id, "person_id", "people", engine):
        logger.warning(f"Person with primary id of {person_id} doesn't exists!")
        person_details = await get_data_from_tmdb_api(
            id=crew["person_id"],
            url="https://api.themoviedb.org/3/person",
            endpoint_name="person"
        )
        person_details = await clean_person_details(person_details["id"], person_details)

        await load_single_row_to_db(
            table_name="people",
            data=person_details,
            primary_key_id=person_details["person_id"],
            engine=engine
        )

    await load_single_row_to_db(
        table_name="movie_crew",
        data=crew | {"movie_id": movie_id},
        primary_key_id=person_id,
        engine=engine
    )

@flow(
    name="Parallel Movie Crew ETL",
    log_prints=True,
    flow_run_name="movie-crew-flow-on-{movie_id}"
)
async def movie_crew_flow(
    movie_id: int,
    movie_crews: List
):
    crew_limit = asyncio.Semaphore(10)

    async def process_crew_with_semaphore(coro):
        async with crew_limit:
            return await coro

    futures = [process_crew_with_semaphore(crew_flow(movie_id, crew, crew["person_id"])) for crew in movie_crews]
    await asyncio.gather(*futures)

@flow(
    name="External Data ETL",
    log_prints=True,
    flow_run_name="external-data-of-movie-{movie_id}"
)
async def external_data_flow(
    movie_id: int,
    wiki_id: str
):
    wiki_soup = await scrape_html_content(
        wiki_id,
        url="https://www.wikidata.org/wiki",
        source="wikidata"
    )

    external_ids = await clean_wikidata(
        wiki_id,
        wiki_soup
    )

    await imdb_ratings_flow(movie_id, external_ids["imdb_id"])
    await metacritic_ratings_flow(movie_id, external_ids["metacritic_id"])
    await rotten_tomatoes_ratings_flow(movie_id, external_ids["rotten_tomatoes_id"])

@flow(
    name="IMDB Rating ETL",
    log_prints=True,
    flow_run_name="imdb-ratings-on-{imdb_id}"
)
async def imdb_ratings_flow(
    movie_id: int,
    imdb_id: str
):
    imdb_soup = await scrape_html_content(
        imdb_id,
        url="https://www.imdb.com/title",
        source="imdb"
    )

    imdb_ratings = await clean_imdb_ratings(
        imdb_id,
        imdb_soup
    )

    await load_single_row_to_db(
        table_name="imdb_details",
        primary_key_id="imdb_id",
        data=imdb_ratings | {"movie_id": movie_id},
        engine=engine
    )

@flow(
    name="Metacritic Rating ETL",
    log_prints=True,
    flow_run_name="metacritic-ratings-on-{metacritic_id}"
)
async def metacritic_ratings_flow(
    movie_id: int,
    metacritic_id: str
):
    metacritic_soup = await scrape_html_content(
        metacritic_id,
        url="https://www.metacritic.com",
        source="metacritic"
    )

    metacritic_ratings = await clean_metacritic_ratings(
        metacritic_id,
        metacritic_soup
    )

    await load_single_row_to_db(
        table_name="metacritic_details",
        primary_key_id="metacritic_id",
        data=metacritic_ratings | {"movie_id": movie_id},
        engine=engine
    )

@flow(
    name="Rotten Tomatoes Rating ETL",
    log_prints=True,
    flow_run_name="rotten_tomatoes-ratings-on-{rotten_tomatoes_id}"
)
async def rotten_tomatoes_ratings_flow(
    movie_id: int,
    rotten_tomatoes_id: str
):
    rotten_tomatoes_soup = await scrape_html_content(
        rotten_tomatoes_id,
        url="https://www.rottentomatoes.com",
        source="rotten_tomatoes"
    )

    rotten_tomatoes_ratings = await clean_rotten_tomatoes_ratings(
        rotten_tomatoes_id,
        rotten_tomatoes_soup
    )

    await load_single_row_to_db(
        table_name="rotten_tomatoes_details",
        primary_key_id="rotten_tomatoes_id",
        data=rotten_tomatoes_ratings | {"movie_id": movie_id},
        engine=engine
    )

@flow(
    name="Movie ETL",
    log_prints=True,
    flow_run_name="movie-flow-on-{movie_id}"
)
async def single_movie_flow(movie_id: int):
    logger = get_run_logger()
    movie_details = await movie_details_flow(movie_id)

    futures = [
        movie_cast_flow(movie_id, movie_details["casts"]),
        movie_crew_flow(movie_id, movie_details["crews"]),
        movie_provder_flow(movie_id, movie_details["watch_providers"]),
    ]

    if movie_details["genres"] != []:
        futures.append(movie_genre_flow(movie_id, movie_details["genres"]))
    else:
        logger.warning("Movie genres doesn't exists!")

    if movie_details["spoken_languages"] != []:
        futures.append(movie_language_flow(movie_id, movie_details["spoken_languages"]))
    else:
        logger.warning("Movie languages doesn't exists!")

    if movie_details["production_companies"] != []:
        futures.append(movie_production_flow(movie_id, movie_details["production_companies"]))
    else:
        logger.warning("Production companies doesn't exists!")

    if movie_details["production_countries"] != []:
        futures.append(movie_production_country_flow(movie_id, movie_details["production_countries"]))
    else:
        logger.warning("Production countries doesn't exists!")

    if movie_details["wiki_id"] != None:
        futures.append(external_data_flow(movie_id, movie_details["wiki_id"]))
    else:
        logger.warning("Wiki ID doesn't exists!")

    try:
        await asyncio.gather(*futures)

    except Exception as e:
        logger.error(f"Error processing movie: {e}")
        logger.warning("Rollback current movie")
        rollback_movie(movie_id, engine)
    
    finally:
        await asyncio.sleep(5)