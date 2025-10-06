import requests
from .config import BASE_URL
from .base import Extractor

class BootstrapStaticExtractor(Extractor):
    def extract(self):
        url = BASE_URL + "bootstrap-static/"
        return requests.get(url).json()

class FixturesExtractor(Extractor):
    def extract(self, future: int = 0):
        url = BASE_URL + "fixtures/"
        if future in (0, 1):
            url += f"?future={future}"
        return requests.get(url).json()

class PlayerSummaryExtractor(Extractor):
    def extract(self, player_id: int):
        url = BASE_URL + f"element-summary/{player_id}/"
        return requests.get(url).json()
