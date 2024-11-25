from typing import List, Dict
from prefect.cache_policies import NONE
from prefect import task

@task(
    name="Load Single Entity to KG",
    cache_policy=NONE
)
def load_entity_to_kg(
    node_label: str,
    node_property: Dict,
    driver
):
    node_property_str = []

    for k, v in node_property.items():
        if v != None:
            if type(v) == str:
                node_property_str.append(f'{k}: "{v}"')
            else:
                node_property_str.append(f"{k}: {v}")
    
    node_property_str = ", ".join(node_property_str)

    try:
        with driver.session() as session:
            session.run(
                f"""CREATE (n:{node_label} {{{node_property_str}}})"""
            )

    except Exception as e:
        raise e

def load_relationship_to_kg(
    relationship_label: str,
    head_label: str,
    tail_label: str,
    head_property_id: Dict,
    tail_property_id: Dict,
    relationship_property: Dict,
    driver
):
    relationship_property_str = []

    for k, v in relationship_property.items():
        if v != None:
            if type(v) == str:
                relationship_property_str.append(f'{k}: "{v}"')
            else:
                relationship_property_str.append(f"{k}: {v}")
    
    relationship_property_str = ", ".join(relationship_property_str)

    try:
        with driver.session() as session:
            session.run(            
                f"""MATCH (h:{head_label}{{{head_property_id}}}), (t:{tail_label}{{{tail_property_id}}})
                CREATE (h)-[r:{relationship_label} {{{relationship_property_str}}}]->(t)"""
            )

    except Exception as e:
        raise e

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