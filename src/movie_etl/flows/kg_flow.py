from neo4j import GraphDatabase
import os
from typing import List, Dict
import pandas as pd
from dotenv import load_dotenv
from prefect import flow

from src.movie_etl.utils.etl import load_to_csv
from src.movie_etl.tasks.kg_task import load_entity_from_csv_to_kg, load_entity_to_kg

load_dotenv()

driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
)

@flow(
    name="Entity Flow",
    log_prints=True,
    validate_parameters=False
)
def entity_flow(
    node_label: str,
    node_property: Dict=None,
    property_columns: List=None,
    path: str=None,
    df: pd.DataFrame=None,
    from_csv: bool=False
):
    if from_csv:
        load_to_csv(path, df, property_columns)

        load_entity_from_csv_to_kg(path, node_label, property_columns, driver)
    
    else:
        load_entity_to_kg(node_label, node_property, driver)