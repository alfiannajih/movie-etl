from typing import List, Dict
from prefect.cache_policies import NONE
from prefect import task, get_run_logger
from neo4j import Driver
import asyncio

from src.movie_etl.utils.etl import parse_property

@task(
    name="Load Single Entity to KG",
    log_prints=True,
    cache_policy=NONE
)
async def load_entity_to_kg(
    node_label: str,
    node_property: Dict,
    driver: Driver,
    date_keys: List=[]
):
    logger = get_run_logger()
    node_property_str = parse_property(node_property, date_keys=date_keys)

    try:
        with driver.session() as session:
            session.run(
                f"""CREATE (n:{node_label} {{{node_property_str}}})""",
                parameters=node_property
            )

    except Exception as e:
        if "already exists with label" in str(e):
            logger.warning(f"Node already exist!")
        else:
            raise e

    await asyncio.sleep(2)

@task(
    name="Load Single Relationship to KG",
    log_prints=True,
    cache_policy=NONE
)
async def load_relationship_to_kg(
    relationship_label: str,
    head_label: str,
    tail_label: str,
    head_property_id: Dict,
    tail_property_id: Dict,
    driver,
    relationship_property: Dict={},
    head_map_key: Dict={},
    tail_map_key: Dict={}
):
    logger = get_run_logger()
    if relationship_property != {}:
        relationship_property_str = parse_property(relationship_property)
    else:
        relationship_property_str = ""

    head_property_str = parse_property(head_property_id, map_keys=head_map_key)
    tail_property_str = parse_property(tail_property_id, map_keys=tail_map_key)

    try:
        with driver.session() as session:
            session.run(
                f"""MATCH (h:{head_label} {{{head_property_str}}}), (t:{tail_label} {{{tail_property_str}}})
                CREATE (h)-[r:{relationship_label} {{{relationship_property_str}}}]->(t)""",
                parameters=head_property_id | tail_property_id | relationship_property
            )

    except Exception as e:
        if "already exists with label" in str(e):
            logger.warning(f"Relationship already exist!")
        else:
            raise e
    
    await asyncio.sleep(2)

@task(
    name="Load Bulk Entity to KG",
    log_prints=True
)
def load_entity_from_csv_to_kg(
    path: str,
    node_label: str,
    property_columns: List,
    driver
):
    node_property = ", ".join([f"{prop}: row.{prop}" for prop in property_columns])

    try:
        with driver.session() as session:
            session.run(
                f"""LOAD CSV WITH HEADERS FROM '{path}' AS row
                MERGE (n:{node_label} {{{node_property}}})"""
            )
    
    except Exception as e:
        raise e

@task(
    name="Load Bulk Relationship to KG",
    log_prints=True
)
def load_relationship_from_csv_to_kg(
    path: str,
    relationship_label: str,
    head_label: str,
    tail_label: str,
    property_columns: List,
    driver
):
    relationship_property = ", ".join([f"{prop}: row.{prop}" for prop in property_columns])

    try:
        with driver.session() as session:
            session.run(
                f"""LOAD CSV WITH HEADERS FROM 'file:///{path}' AS row
                MATCH (h:{head_label}{{head_id: row.id}})
                MATCH (t:{tail_label}{{tail_id: row.id}})
                MERGE (r:{relationship_label} {{{relationship_property}}})"""
            )
    
    except Exception as e:
        raise e