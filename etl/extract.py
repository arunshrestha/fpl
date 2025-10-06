import requests
from typing import Dict, Any

BASE_URL = "https://fantasy.premierleague.com/api/"

def fetch_bootstrap_static() -> Dict[str, Any]:
    """Fetch bootstrap-static data."""
    url = BASE_URL + "bootstrap-static/"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def fetch_fixtures() -> list[Dict]:
    """Fetch all season fixtures."""
    url = BASE_URL + "fixtures/"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def fetch_player_history(player_id: int) -> Dict[str, Any]:
    """Fetch player detailed history (current season and past)."""
    url = BASE_URL + f"element-summary/{player_id}/"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()
