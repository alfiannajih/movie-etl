from sqlalchemy.engine.base import Engine
import re
from bs4 import BeautifulSoup
from datetime import date, timedelta
from prefect.runtime import flow_run
import pandas as pd
from typing import List

gender_dict = {
    0: "Not specified",
    1: "Female",
    2: "Male",
    3: "Non-binary"
}

def map_gender(
    gender_id: int
) -> str:
    return gender_dict[gender_id]

def is_primary_key_exist_in_table(
    primary_key,
    primary_key_name: str,
    table_name: str,
    engine: Engine
):
    connection = engine.raw_connection()
    if type(primary_key) == str:
        primary_key = f"'{primary_key}'"
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT {primary_key_name} FROM {table_name} WHERE {primary_key_name} = {primary_key}"
        )
        result = cursor.fetchone()
        if result != None:
            return True
        else:
            return False
        
def extract_metacritic_data(
    reviews_soup: BeautifulSoup
):
    review_score = reviews_soup.find("div", class_="c-siteReviewScore").text

    review_sentiments = reviews_soup.find("div", class_="c-reviewsStats")

    positive_sentiments, neutral_sentiments, negative_sentiments = review_sentiments.find_all("div")

    num_positive = int(re.search(r"\d+(?= (Reviews|Ratings|Review|Rating))", positive_sentiments.text).group())
    num_neutral = int(re.search(r"\d+(?= (Reviews|Ratings|Review|Rating))", neutral_sentiments.text).group())
    num_negative = int(re.search(r"\d+(?= (Reviews|Ratings|Review|Rating))", negative_sentiments.text).group())

    num_reviews = num_positive + num_neutral + num_negative

    percent_positive = int(re.search(r"\d+(?=%)", positive_sentiments.text).group())
    percent_neutral = int(re.search(r"\d+(?=%)", neutral_sentiments.text).group())
    percent_negative = int(re.search(r"\d+(?=%)", negative_sentiments.text).group())

    return {
        "review_score": int(float(review_score)*10) if "." in review_score else int(float(review_score)),
        "num_reviews": num_reviews,
        "percent_positive": percent_positive,
        "percent_neutral": percent_neutral,
        "percent_negative": percent_negative
    }

def rollback_movie(
    movie_id: int,
    engine: Engine
):
    tables = [
        "movie_production",
        "movie_language",
        "movie_genre",
        "movie_cast",
        "movie_crew",
        "movie_provider",
        "production_country",
        "rotten_tomatoes_details",
        "imdb_details",
        "metacritic_details"
    ]

    connection = engine.raw_connection()

    for table in tables:
        with connection.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {table} where movie_id = {movie_id}"
            )

        connection.commit()
    
    with connection.cursor() as cursor:
        cursor.execute(
            f"DELETE FROM movies where movie_id = {movie_id}"
        )
    connection.commit()

    connection.close()

def get_previous_week(
    current_date: date=date.today()
) -> date:
    previous_date = (current_date - timedelta(days=7))

    return previous_date

def generate_flow_run_name():
    parameters = flow_run.parameters
    start_date = parameters["start_date"]
    end_date = parameters["end_date"]

    if start_date is None or end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")
        start_date = get_previous_week().strftime("%Y-%m-%d")

    return f"etl-flow-on-{start_date}--{end_date}"

def load_to_csv(
    path: str,
    df: pd.DataFrame,
    property_columns: List
):
    df[property_columns].to_csv(path, index=False)