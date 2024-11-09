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