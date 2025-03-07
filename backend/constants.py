
BASE_URL = "https://statsapi.mlb.com/api/v1/"
database = "sqlite:///playbyplay.db"

def decode_base_state(state_code):
    """Convert base state code to readable format"""
    base_states = {
        0: "Empty",
        1: "1st",
        2: "2nd",
        3: "1st & 2nd",
        4: "3rd",
        5: "1st & 3rd",
        6: "2nd & 3rd",
        7: "Bases Loaded",
    }
    return base_states.get(state_code, "N/A")
