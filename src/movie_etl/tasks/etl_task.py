import numpy as np
from typing import List, Dict
import requests
from prefect import task
import os

from src.movie_etl.utils.etl import map_gender

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {os.getenv("TMDB_API_KEY")}"
}

@task(
    name="Retrieve Movie IDs",
    log_prints=True,
    retries=2,
    task_run_name="get-movie-ids-on-{start_date}--{end_date}"
)
def get_movie_ids(
    start_date: str="2020-01-01",
    end_date: str="2020-02-01",
    url: str="https://api.themoviedb.org/3/discover/movie",
    vote_count_minimum: int=10,
    original_language: str="en"
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
            headers=headers,
            params=params
        ).json()

        current_ids = [movie["id"] for movie in response["results"]]
        movie_ids.extend(current_ids)
        
        page += 1
        total_pages = response["total_pages"]
        
    # logger.info(f"Get {len(movie_ids)} movie_ids")
    return movie_ids

@task(
    name="Retrieve Data from TMDB API",
    log_prints=True,
    retries=2,
    task_run_name="retrieve-{endpoint}-of-{id}"
)
def get_data_from_tmdb_api(
    id: int,
    url: str,
    endpoint: str,
    params: Dict=None
) -> Dict:
    if id == None:
        response = requests.get(
            url,
            headers=headers,
            params=params
        ).json()
    else:
        response = requests.get(
            f"{url}/{id}",
            headers=headers,
            params=params
        ).json()

    return response

@task(
    name="Clean Movie Details",
    log_prints=True,
    task_run_name="clean-movie-details-of-{movie_id}"
)
def clean_movie_details(
    movie_id: int,
    movie_details: Dict
) -> Dict:
    casts = [
        {
            "person_id": cast["id"],
            "character": cast["character"]
        } for cast in movie_details["credits"]["cast"]
    ]

    crews = [
        {
            "person_id": crew["id"],
            "job": crew["job"],
            "department": crew["department"]
        } for crew in movie_details["credits"]["crew"]
    ]

    production_companies = [company["id"] for company in movie_details["production_companies"]]

    spoken_languages = [language["iso_639_1"] for language in movie_details["spoken_languages"]]

    return {
        "collection_id": movie_details["belongs_to_collection"]["id"] if movie_details["belongs_to_collection"] != None else None,
        "movie_id": movie_details["id"],
        "imdb_id": movie_details["imdb_id"],
        "title": movie_details["title"],
        "overview": movie_details["overview"] if movie_details["overview"] != "" else None,
        "release_date": movie_details["release_date"],
        "popularity": movie_details["popularity"] if movie_details["popularity"] != 0 else None,
        "vote_average": movie_details["vote_average"] if movie_details["vote_average"] != 0 else None,
        "vote_count": movie_details["vote_count"] if movie_details["vote_count"] != 0 else None,
        "budget": movie_details["budget"] if movie_details["budget"] != 0 else None,
        "revenue": movie_details["revenue"] if movie_details["revenue"] != 0 else None,
        "runtime": movie_details["runtime"] if movie_details["runtime"] != 0 else None,
        "genres": movie_details["genres"],
        "casts": casts,
        "crews": crews,
        "production_companies": production_companies,
        "spoken_languages": spoken_languages
    }

@task(
    name="Clean Collection Details",
    log_prints=True,
    task_run_name="clean-collection-details-of-{collection_id}"
)
def clean_collection_details(
    collection_id: int,
    collection_details: Dict
) -> Dict:
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
def clean_company_details(
    company_id: int,
    company_details: Dict
) -> Dict:
    return {
        "company_id": company_details["id"],
        "parent_company_id": company_details["parent_company"]["id"] if company_details["parent_company"] != None else None,
        "name": company_details["name"],
        "description": company_details["description"] if company_details["description"] != "" else None,
        "country": company_details["origin_country"] if company_details["origin_country"] != "" else None,
        "head_quarters": company_details["headquarters"] if company_details["headquarters"] != "" else None
    }

@task(
    name="Clean Person Details",
    log_prints=True,
    task_run_name="clean-person-details-of-{person_id}"
)
def clean_person_details(
    person_id: int,
    person_details: Dict
) -> Dict:
    return {
        "person_id": person_details["id"],
        "imdb_id": person_details["imdb_id"],
        "name": person_details["name"],
        "gender": map_gender(person_details["gender"]),
        "biography": person_details["biography"] if person_details["biography"] != "" else None,
        "place_of_birth": person_details["place_of_birth"] if person_details["place_of_birth"] != "" else None,
        "birthday": person_details["birthday"] if person_details["birthday"] != "" else None,
        "deathday": person_details["deathday"] if person_details["deathday"] != "" else None,
        "popularity": person_details["popularity"] if person_details["popularity"] != 0 else None
    }

# @task(
#     name="Retrieve Movie Details",
#     log_prints=True,
#     retries=2,
#     task_run_name="get-movie-details-of-{movie_id}"
# )
# def get_movie_details(
#     movie_id: int,
#     url: str="https://api.themoviedb.org/3/movie"
# ) -> Dict:
#     # logger = get_run_logger()
#     # logger.info("Start retrieving movie details for movie_id: " + str(movie_id))
    
#     params = {
#         "append_to_response": "credits"
#     }

#     response = requests.get(
#         f"{url}/{movie_id}",
#         headers=headers,
#         params=params
#     ).json()

#     casts = [
#         {
#             "person_id": cast["id"],
#             "character": cast["character"]
#         } for cast in response["credits"]["cast"]
#     ]

#     crews = [
#         {
#             "person_id": crew["id"],
#             "job": crew["job"],
#             "department": crew["department"]
#         } for crew in response["credits"]["crew"]
#     ]

#     production_companies = [company["id"] for company in response["production_companies"]]

#     spoken_languages = [language["iso_639_1"] for language in response["spoken_languages"]]

#     return {
#         "collection_id": response["belongs_to_collection"]["id"] if response["belongs_to_collection"] != None else None,
#         "movie_id": movie_id,
#         "imdb_id": response["imdb_id"],
#         "title": response["title"],
#         "overview": response["overview"],
#         "release_date": response["release_date"],
#         "popularity": response["popularity"],
#         "vote_average": response["vote_average"],
#         "vote_count": response["vote_count"],
#         "budget": response["budget"] if response["budget"] != 0 else None,
#         "revenue": response["revenue"] if response["revenue"] !=0 else None,
#         "runtime": response["runtime"],
#         "genres": response["genres"],
#         "casts": casts,
#         "crews": crews,
#         "production_companies": production_companies,
#         "spoken_languages": spoken_languages
#     }

# @task(
#     name="Retrieve Collection Details",
#     log_prints=True,
#     retries=2,
#     task_run_name="get-collection-details-of-{collection_id}"
# )
# def get_collection_details(
#     collection_id: int,
#     url: str="https://api.themoviedb.org/3/collection"
# ) -> Dict:
#     # logger = get_run_logger()
#     # logger.info("Start retrieving collection details for collection_id: " + str(collection_id))
#     response = requests.get(
#         f"{url}/{collection_id}",
#         headers=headers
#     ).json()

#     return {
#         "collection_id": collection_id,
#         "name": response["name"],
#         "overview": response["overview"]
#     }

# @task(
#     name="Retrieve Company Details",
#     log_prints=True,
#     retries=2,
#     task_run_name="get-company-details-of-{company_id}"
# )
# def get_company_details(
#     company_id: int,
#     url: str="https://api.themoviedb.org/3/company"
# ) -> Dict:
#     response = requests.get(
#         f"{url}/{company_id}",
#         headers=headers
#     ).json()
    
#     return {
#         "company_id": company_id,
#         "parent_company_id": response["parent_company"]["id"] if response["parent_company"] != None else None,
#         "name": response["name"],
#         "description": response["description"] if response["description"] != "" else None,
#         "country": response["origin_country"],
#         "head_quarters": response["headquarters"] if response["headquarters"] != "" else None
#     }

# @task(
#     name="Retrieve Person Details",
#     log_prints=True,
#     retries=2,
#     task_run_name="get-person-details-of-{person_id}"
# )
# def get_person_details(
#     person_id: int,
#     url: str="https://api.themoviedb.org/3/person"
# ) -> Dict:
#     response = requests.get(
#         f"{url}/{person_id}",
#         headers=headers
#     ).json()

#     return {
#         "person_id": person_id,
#         "imdb_id": response["imdb_id"],
#         "name": response["name"],
#         "gender": map_gender(response["gender"]),
#         "biography": response["biography"] if response["biography"] != "" else None,
#         "place_of_birth": response["place_of_birth"],
#         "birthday": response["birthday"],
#         "deathday": response["deathday"],
#         "popularity": response["popularity"]
#     }