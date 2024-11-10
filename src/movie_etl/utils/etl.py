from sqlalchemy.engine.base import Engine

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