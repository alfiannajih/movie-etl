import requests
from prefect import task, get_run_logger
from dotenv import load_dotenv
import os
from typing import List, Dict
import numpy as np
from sqlalchemy import create_engine

from utils.etl import map_gender

load_dotenv()

engine = create_engine(os.getenv("DB_CONNECTION"))
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {os.getenv("TMDB_API_KEY")}"
}

def get_movie_ids(
    start_date: str="2020-01-01",
    end_date: str="2020-02-01",
    url: str="https://api.themoviedb.org/3/discover/movie",
    vote_count_minimum: int=10,
    original_language: str="en"
) -> List:
    print("Start retrieving movie_ids")
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
        
    print(f"Get {len(movie_ids)} movie_ids")
    return movie_ids

def get_movie_details(
    movie_id: int,
    url: str="https://api.themoviedb.org/3/movie"
) -> Dict:
    params = {
        "append_to_response": "credits"
    }

    response = requests.get(
        f"{url}/{movie_id}",
        headers=headers,
        params=params
    ).json()

    casts = [
        {
            "person_id": cast["id"],
            "character": cast["character"]
        } for cast in response["credits"]["cast"]
    ]

    crews = [
        {
            "person_id": crew["id"],
            "job": crew["job"],
            "department": crew["department"]
        } for crew in response["credits"]["crew"]
    ]

    production_companies = [company["id"] for company in response["production_companies"]]

    spoken_languages = [language["iso_639_1"] for language in response["spoken_languages"]]

    if response["belongs_to_collection"] != None:
        collection_id = response["belongs_to_collection"]["id"]

        if not is_primary_key_exist_in_table(collection_id, "collection_id", "movie_collections"):
            collection_details = get_collection_details(collection_id)
            load_collection_details_to_db(collection_details)
    else:
        collection_id = None

    return {
        "collection_id": collection_id,
        "movie_id": movie_id,
        "imdb_id": response["imdb_id"],
        "title": response["title"],
        "overview": response["overview"],
        "release_date": response["release_date"],
        "popularity": response["popularity"],
        "vote_average": response["vote_average"],
        "vote_count": response["vote_count"],
        "budget": response["budget"] if response["budget"] != 0 else None,
        "revenue": response["revenue"] if response["revenue"] !=0 else None,
        "runtime": response["runtime"],
        "genres": response["genres"],
        "casts": casts,
        "crews": crews,
        "production_companies": production_companies,
        "spoken_languages": spoken_languages
    }

def is_primary_key_exist_in_table(
    primary_key: int,
    primary_key_name: str,
    table_name: str
):
    connection = engine.raw_connection()
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT {primary_key_name} FROM {table_name} WHERE {primary_key_name} = {primary_key}"
        )
        result = cursor.fetchone()
        if result != None:
            return True
        else:
            return False

def get_collection_details(
    collection_id: int,
    url: str="https://api.themoviedb.org/3/collection"
) -> Dict:
    response = requests.get(
        f"{url}/{collection_id}",
        headers=headers
    ).json()

    return {
        "collection_id": collection_id,
        "name": response["name"],
        "overview": response["overview"]
    }

def get_person_details(
    person_id: int,
    url: str="https://api.themoviedb.org/3/person"
) -> Dict:
    response = requests.get(
        f"{url}/{person_id}",
        headers=headers
    ).json()

    return {
        "person_id": person_id,
        "imdb_id": response["imdb_id"],
        "name": response["name"],
        "gender": map_gender(response["gender"]),
        "biography": response["biography"] if response["biography"] != "" else None,
        "place_of_birth": response["place_of_birth"],
        "birthday": response["birthday"],
        "deathday": response["deathday"],
        "popularity": response["popularity"]
    }

def load_collection_details_to_db(
    collection_details: Dict
):
    connection = engine.raw_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO movie_collections (
                    collection_id,
                    name,
                    overview
                ) VALUES (
                    %(collection_id)s,
                    %(name)s,
                    %(overview)s
                )""",
                collection_details
            )
        connection.commit()
    except Exception as e:
        print("Error inserting row:", e)
    finally:
        connection.close()

def add_genre_to_db(
    genre: Dict
):
    connection = engine.raw_connection()
    with connection.cursor() as cursor:
        cursor.execute(
            """INSERT INTO genres (
                genre_id,
                genre
            ) VALUES (
                %(id)s,
                %(name)s
            )""",
            genre
        )
        connection.commit()
    connection.close()

def load_genre_to_db(
    movie_genre: Dict
):
    connection = engine.raw_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO movie_genre (
                    genre_id,
                    movie_id
                ) VALUES (
                    %(genre_id)s,
                    %(movie_id)s
                )""",
                movie_genre
            )
            connection.commit()
    except Exception as e:
        print("Error inserting row:", e)
    finally:
        connection.close()

def load_movie_details_to_db(
    movie_details: Dict
):
    connection = engine.raw_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO movies (
                    movie_id,
                    collection_id,
                    imdb_id,
                    title,
                    overview,
                    release_date,
                    popularity,
                    vote_average,
                    vote_count,
                    budget,
                    revenue,
                    runtime
                ) VALUES (
                    %(movie_id)s,
                    %(collection_id)s,
                    %(imdb_id)s,
                    %(title)s,
                    %(overview)s,
                    %(release_date)s,
                    %(popularity)s,
                    %(vote_average)s,
                    %(vote_count)s,
                    %(budget)s,
                    %(revenue)s,
                    %(runtime)s
                )""",
                movie_details
            )
        connection.commit()
    except Exception as e:
        print("Error inserting row:", e)
    finally:
        connection.close()

def add_person_to_db(
    person_details: Dict
):
    connection = engine.raw_connection()
    with connection.cursor() as cursor:
        cursor.execute(
            """INSERT INTO people (
                person_id,
                imdb_id,
                name,
                gender,
                biography,
                place_of_birth,
                birthday,
                deathday,
                popularity
            ) VALUES (
                %(person_id)s,
                %(imdb_id)s,
                %(name)s,
                %(gender)s,
                %(biography)s,
                %(place_of_birth)s,
                %(birthday)s,
                %(deathday)s,
                %(popularity)s
            )""",
            person_details
        )
    connection.commit()
    connection.close()

def load_movie_cast_to_db(
    cast: Dict
):
    connection = engine.raw_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO movie_cast (
                    person_id,
                    movie_id,
                    character
                ) VALUES (
                    %(person_id)s,
                    %(movie_id)s,
                    %(character)s
                )""",
                cast
            )
        connection.commit()
    except Exception as e:
        print("Error inserting row:", e)
    finally:
        connection.close()

def load_movie_crew_to_db(
    crew: Dict
):
    connection = engine.raw_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO movie_crew (
                    person_id,
                    movie_id,
                    job,
                    department
                ) VALUES (
                    %(person_id)s,
                    %(movie_id)s,
                    %(job)s,
                    %(department)s
                )""",
                crew
            )
        connection.commit()
    except Exception as e:
        print("Error inserting row:", e)
    finally:
        connection.close()

def get_company_details(
    company_id: int,
    url: str="https://api.themoviedb.org/3/company"
) -> Dict:
    response = requests.get(
        f"{url}/{company_id}",
        headers=headers
    ).json()
    
    return {
        "company_id": company_id,
        "parent_company_id": response["parent_company"]["id"] if response["parent_company"] != None else None,
        "name": response["name"],
        "description": response["description"] if response["description"] != "" else None,
        "country": response["origin_country"],
        "head_quarters": response["headquarters"] if response["headquarters"] != "" else None
    }

def load_company_to_db(
    company_details: Dict
):
    connection = engine.raw_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO companies (
                    company_id,
                    parent_company_id,
                    name,
                    description,
                    country,
                    head_quarters
                ) VALUES (
                    %(company_id)s,
                    %(parent_company_id)s,
                    %(name)s,
                    %(description)s,
                    %(country)s,
                    %(head_quarters)s
                )""",
                company_details
            )
        connection.commit()
    except Exception as e:
        print("Error inserting row:", e)
    finally:
        connection.close()
    
    connection.close()

def load_movie_production_to_db(
    movie_production: Dict
):
    connection = engine.raw_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO movie_production (
                    company_id,
                    movie_id
                ) VALUES (
                    %(company_id)s,
                    %(movie_id)s
                )""",
                movie_production
            )
        connection.commit()
    except Exception as e:
        print("Error inserting row:", e)
    finally:
        connection.close()

if __name__ == "__main__":
    movie_ids = get_movie_ids(start_date="2024-09-01", end_date="2024-11-01", vote_count_minimum=50)[:1]
    
    for movie_id in movie_ids:
        movie_details = get_movie_details(movie_id)
        load_movie_details_to_db(movie_details)

        for genre in movie_details["genres"]:
            if not is_primary_key_exist_in_table(genre["id"], "genre_id", "genres"):
                add_genre_to_db(genre)

            load_genre_to_db({"genre_id": genre["id"], "movie_id": movie_id})

        for cast in movie_details["casts"]:
            if not is_primary_key_exist_in_table(cast["person_id"], "person_id", "people"):
                person_details = get_person_details(cast["person_id"])

                add_person_to_db(person_details)
            
            load_movie_cast_to_db(cast | {"movie_id": movie_id})

        for crew in movie_details["crews"]:
            if not is_primary_key_exist_in_table(crew["person_id"], "person_id", "people"):
                person_details = get_person_details(crew["person_id"])

                add_person_to_db(person_details)
            
            load_movie_crew_to_db(crew | {"movie_id": movie_id})

        for company_id in movie_details["production_companies"]:
            companies_to_add = []
            if not is_primary_key_exist_in_table(company_id, "company_id", "companies"):
                company_details = get_company_details(company_id)
                companies_to_add.append(company_details)
                parent_company_id = company_details["parent_company_id"]

                while parent_company_id != None:
                    if not is_primary_key_exist_in_table(parent_company_id, "company_id", "companies"):
                        parent_company_details = get_company_details(parent_company_id)
                    
                    companies_to_add.append(parent_company_details)
                    parent_company_id = companies_to_add[-1]["parent_company_id"]
                    breakpoint()

                for i in range(len(companies_to_add)-1, -1, -1):
                    load_company_to_db(companies_to_add[i])
                
            load_movie_production_to_db({"company_id": company_id, "movie_id": movie_id})