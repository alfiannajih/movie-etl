import numpy as np
from typing import List, Dict, Tuple
import requests
import os
import asyncio
from sqlalchemy.engine.base import Engine
from prefect import task, get_run_logger
from prefect.cache_policies import NONE
from bs4 import BeautifulSoup
import re
from collections import defaultdict

from src.movie_etl.utils.etl import map_gender, extract_metacritic_data

tmdb_headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {os.getenv("TMDB_API_KEY")}"
}

headers = {
    'User-Agent': 'Mozilla/5.0'
}

@task(
    name="Retrieve Movie IDs",
    log_prints=True,
    retries=2,
    task_run_name="get-movie-ids-on-{start_date}--{end_date}"
)
async def get_movie_ids(
    start_date: str=None,
    end_date: str=None,
    url: str="https://api.themoviedb.org/3/discover/movie",
    vote_count_minimum: int=10,
    original_language: str=""
) -> List:
    # logger = get_run_logger()
    # logger.info("Start retrieving movie_ids")

    movie_ids = []
    page = 1
    total_pages = np.inf
    
    while page <= total_pages:
        params = {
            "include_adult": False,
            "include_video": False,
            "language": "en-US",
            "page": page,
            "primary_release_date.gte": start_date,
            "primary_release_date.lte": end_date,
            "sort_by": "primary_release_date.asc",
            "vote_count.gte": vote_count_minimum,
            "with_original_language": original_language
        }

        response = requests.get(
            url,
            headers=tmdb_headers,
            params=params
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise e

        current_ids = [movie["id"] for movie in response.json()["results"]]
        movie_ids.extend(current_ids)
        
        page += 1
        total_pages = response.json()["total_pages"]
        
    # logger.info(f"Get {len(movie_ids)} movie_ids")
    await asyncio.sleep(2)
    return movie_ids

@task(
    name="Retrieve Data from TMDB API",
    log_prints=True,
    retries=2,
    task_run_name="retrieve-tmdb-data-id-{id}-from-{endpoint_name}"
)
async def get_data_from_tmdb_api(
    id: int,
    url: str,
    endpoint_name: str,
    params: Dict=None
) -> Dict:
    if id == None:
        response = requests.get(
            url,
            headers=tmdb_headers,
            params=params
        )
    else:
        response = requests.get(
            f"{url}/{id}",
            headers=tmdb_headers,
            params=params
        )
    
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise e

    await asyncio.sleep(2)
    return response.json()

@task(
    name="Scrape Data from HTML Content",
    log_prints=True,
    retries=3,
    retry_delay_seconds=3,
    task_run_name="scrape-html-content-{id}-from-{source}"
)
async def scrape_html_content(
    id: str,
    url: str,
    source: str,
    suffix: str=None
) -> BeautifulSoup:
    # logger = get_run_logger()

    if suffix != None:
        response = requests.get(
            f"{url}/{id}/{suffix}",
            headers=headers
        )
    else:
        response = requests.get(
            f"{url}/{id}",
            headers=headers
        )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # logger.error(f"Error scraping data from {source}: {e}", exc_info=True)
        raise e

    await asyncio.sleep(2)
    return BeautifulSoup(response.content, "html.parser")

@task(
    name="Clean Movie Details",
    log_prints=True,
    task_run_name="clean-movie-details-of-{movie_id}"
)
async def clean_movie_details(
    movie_id: int,
    movie_details: Dict
) -> Dict:
    casts = [
        {
            "person_id": cast["id"],
            "name": cast["name"],
            "gender": map_gender(cast["gender"]),
            "character": cast["character"] #if cast["character"] != "" else None
        } for cast in movie_details["credits"]["cast"]
    ]

    crews = [
        {
            "person_id": crew["id"],
            "name": crew["name"],
            "gender": map_gender(crew["gender"]),
            "job": crew["job"],
            "department": crew["department"]
        } for crew in movie_details["credits"]["crew"]
    ]

    production_companies = [company["id"] for company in movie_details["production_companies"]]

    spoken_languages = [language["iso_639_1"] for language in movie_details["spoken_languages"]]

    production_countries = [country["iso_3166_1"] for country in movie_details["production_countries"]]

    genres = [genre["id"] for genre in movie_details["genres"]]

    watch_providers = movie_details["watch/providers"]

    await asyncio.sleep(2)
    return {
        "collection_id": movie_details["belongs_to_collection"]["id"] if movie_details["belongs_to_collection"] != None else None,
        "movie_id": movie_details["id"],
        "title": movie_details["title"],
        "overview": movie_details["overview"] if movie_details["overview"] != "" else None,
        "release_date": movie_details["release_date"],
        "popularity": movie_details["popularity"] if movie_details["popularity"] != 0 else None,
        "budget": movie_details["budget"] if movie_details["budget"] != 0 else None,
        "revenue": movie_details["revenue"] if movie_details["revenue"] != 0 else None,
        "runtime": movie_details["runtime"] if movie_details["runtime"] != 0 else None,
        "wiki_id": movie_details["external_ids"]["wikidata_id"],
        "production_countries": production_countries,
        "genres": genres,
        "casts": casts,
        "crews": crews,
        "production_companies": production_companies,
        "spoken_languages": spoken_languages,
        "watch_providers": watch_providers
    }

@task(
    name="Clean Collection Details",
    log_prints=True,
    task_run_name="clean-collection-details-of-{collection_id}"
)
async def clean_collection_details(
    collection_id: int,
    collection_details: Dict
) -> Dict:
    await asyncio.sleep(2)
    return {
        "collection_id": collection_details["id"],
        "name": collection_details["name"],
        "overview": collection_details["overview"] if collection_details["overview"] != "" else None
    }

@task(
    name="Clean Company Details",
    log_prints=True,
    task_run_name="clean-company-details-of-{company_id}"
)
async def clean_company_details(
    company_id: int,
    company_details: Dict
) -> Dict:
    await asyncio.sleep(2)
    return {
        "company_id": company_details["id"],
        "parent_company_id": company_details["parent_company"]["id"] if company_details["parent_company"] != None else None,
        "name": company_details["name"],
        "description": company_details["description"] if company_details["description"] != "" else None,
        "country_id": company_details["origin_country"] if company_details["origin_country"] != "" else None,
        "head_quarters": company_details["headquarters"] if company_details["headquarters"] != "" else None
    }

@task(
    name="Clean Person Details",
    log_prints=True,
    task_run_name="clean-person-details-of-{person_id}"
)
async def clean_person_details(
    person_id: int,
    person_details: Dict
) -> Dict:
    await asyncio.sleep(2)
    return {
        "person_id": person_details["id"],
        "name": person_details["name"],
        "gender": map_gender(person_details["gender"]),
        "biography": person_details["biography"] if person_details["biography"] != "" else None,
        "place_of_birth": person_details["place_of_birth"] if person_details["place_of_birth"] != "" else None,
        "birthday": person_details["birthday"] if person_details["birthday"] != "" else None,
        "deathday": person_details["deathday"] if person_details["deathday"] != "" else None,
        "popularity": person_details["popularity"] if person_details["popularity"] != 0 else None
    }

@task(
    name="Clean Watch Providers",
    log_prints=True,
    task_run_name="clean-watch-providers-of-{movie_id}"
)
async def clean_watch_providers(
    movie_id: int,
    watch_providers: Dict
) -> List[Tuple]:
    providers = defaultdict(lambda: {"buy": [], "rent": [], "subscription": []})

    for region, details in watch_providers["results"].items():
        for key in ['buy', 'rent', 'flatrate']:
            if key in details:
                for provider in details[key]:
                    provider_id = provider['provider_id']
                    if key == 'flatrate':
                        providers[provider_id]['subscription'].append(region)
                    else:
                        providers[provider_id][key].append(region)
    for provider in providers.values():
        provider = {key: value for key, value in provider.items() if value}

    await asyncio.sleep(2)
    return providers

@task(
    name="Clean Movie Genres",
    log_prints=True,
    task_run_name="clean-genres-of-{movie_id}"
)
async def clean_genres(
    movie_genres: List,
    movie_id
) -> List[Tuple]:
    genres = [(movie_id, genre_id) for genre_id in movie_genres]

    await asyncio.sleep(2)
    return genres

@task(
    name="Clean Movie Languages",
    log_prints=True,
    task_run_name="clean-languages-of-{movie_id}"
)
async def clean_languages(
    movie_languages: List,
    movie_id
) -> List[Tuple]:
    languages = [(movie_id, language_id) for language_id in movie_languages]

    await asyncio.sleep(2)
    return languages

@task(
    name="Clean Production Countries",
    log_prints=True,
    task_run_name="clean-languages-of-{movie_id}"
)
async def clean_production_countries(
    production_countries: List,
    movie_id
) -> List[Tuple]:
    countries = [(movie_id, language_id) for language_id in production_countries]

    await asyncio.sleep(2)
    return countries

@task(
    name="Clean Wikidata",
    log_prints=True,
    task_run_name="clean-wikidata-of-{wiki_id}"
)
async def clean_wikidata(
    wiki_id: str,
    soup: BeautifulSoup
) -> Dict:
    imdb_id = soup.find("div", id="P345").find("a", class_="wb-external-id external").text
    metacritic_id = soup.find("div", id="P1712").find("a", class_="wb-external-id external").text
    rotten_tomatoes_id = soup.find("div", id="P1258").find("a", class_="wb-external-id external").text

    await asyncio.sleep(2)
    return {
        "imdb_id": imdb_id,
        "metacritic_id": metacritic_id,
        "rotten_tomatoes_id": rotten_tomatoes_id
    }

@task(
    name="Clean IMDB Ratings",
    log_prints=True,
    task_run_name="clean-imdb-rating-of-{imdb_id}"
)
async def clean_imdb_ratings(
    imdb_id: str,
    soup: BeautifulSoup
) -> Dict:
    review_sec = soup.find("div", class_="sc-3a4309f8-1 dOjKRs")

    score = review_sec.find("span", class_="sc-d541859f-1 imUuxf").text
    n_score = review_sec.find("div", class_="sc-d541859f-3 dwhNqC").text

    magnitude_dict = {
        "K": 1e3,
        "M": 1e6
    }

    if n_score[-1] in magnitude_dict:
        num, magnitude = n_score[:-1], n_score[-1]
        num_score = float(num) * magnitude_dict[magnitude]

    else:
        num_score = float(n_score)

    await asyncio.sleep(2)
    return {
        "imdb_id": imdb_id,
        "user_score": int(float(score)*10),
        "num_user": int(num_score)
    }

@task(
    name="Clean Metacritic Ratings",
    log_prints=True,
    task_run_name="clean-metacritic-rating-of-{metacritic_id}"
)
async def clean_metacritic_ratings(
    metacritic_id: str,
    soup: BeautifulSoup
) -> Dict:
    review_sec = soup.find_all("div", class_="c-reviewsOverview_overviewDetails")

    try:
        critic_reviews, user_reviews = review_sec[0], review_sec[2]

    except:
        critic_reviews, user_reviews = review_sec[0], review_sec[1]

    critic_scores = extract_metacritic_data(critic_reviews)
    user_scores = extract_metacritic_data(user_reviews)

    return {
        "metacritic_id": metacritic_id,
        "critic_score": critic_scores["review_score"],
        "num_critic": critic_scores["num_reviews"],
        "critic_positive": critic_scores["percent_positive"],
        "critic_neutral": critic_scores["percent_neutral"],
        "critic_negative": critic_scores["percent_negative"],
        "user_score": user_scores["review_score"],
        "num_user": user_scores["num_reviews"],
        "user_positive": user_scores["percent_positive"],
        "user_neutral": user_scores["percent_neutral"],
        "user_negative": user_scores["percent_negative"]
    }

@task(
    name="Clean Rotten Tomatoes Ratings",
    log_prints=True,
    task_run_name="clean-rotten-tomatoes-rating-of-{rotten_tomatoes_id}"
)
async def clean_rotten_tomatoes_ratings(
    rotten_tomatoes_id: str,
    soup: BeautifulSoup
) -> Dict:
    review_sec = soup.find("div", class_="media-scorecard")

    try:
        critic_score = re.search(r"\d+(?=%)", review_sec.find("rt-text", slot="criticsScore").text).group()
        num_critic = re.search(r"\d[\d,]*", review_sec.find("rt-link", slot="criticsReviews").text).group().replace(",", "")
    except:
        critic_score = None
        num_critic = None

    try:
        user_score = re.search(r"\d+(?=%)", review_sec.find("rt-text", slot="audienceScore").text).group()
        num_user = re.search(r"\d[\d,]*", review_sec.find("rt-link", slot="audienceReviews").text).group().replace(",", "")
    except:
        user_score = None
        num_user = None

    return {
        "rotten_tomatoes_id": rotten_tomatoes_id,
        "critic_score": critic_score,
        "num_critic": num_critic,
        "user_score": user_score,
        "num_user": num_user
    }

@task(
    name="Load Single Row to DB",
    log_prints=True,
    task_run_name="load-data-id-{primary_key_id}-to-db-{table_name}",
    retries=2,
    retry_delay_seconds=2,
    cache_policy=NONE
)
async def load_single_row_to_db(
    table_name: str,
    primary_key_id: int,
    data: Dict,
    engine: Engine
):
    logger = get_run_logger()
    connection = engine.raw_connection()
    
    column_names = ", ".join(data.keys())
    column_values = ", ".join([f"%({key})s" for key in data.keys()])
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO {table_name} (
                    {column_names}
                ) VALUES (
                    {column_values}
                )""",
                data
            )
        connection.commit()
    
    except Exception as e:
        if "duplicate key value violates unique constraint" in str(e):
            logger.warning(f"Row already exist!")
        else:
            raise e
    
    finally:
        connection.close()

    await asyncio.sleep(2)

@task(
    name="Load Multi Row to DB",
    log_prints=True,
    task_run_name="load-multi-row-data-to-db-{table_name}",
    cache_policy=NONE
)
async def load_multi_row_to_db(
    table_name: str,
    columns: List,
    data: List,
    engine: Engine
):
    logger = get_run_logger()
    connection = engine.raw_connection()
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO {table_name}
                ({", ".join(columns)})
                VALUES
                {str(data)[1:-1]}
                ON CONFLICT DO NOTHING"""
            )
        connection.commit()

    except Exception as e:
        logger.error(f"Error inserting row: {e}")
        raise e
    
    finally:
        connection.close()

    await asyncio.sleep(2)