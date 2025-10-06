from abc import ABC, abstractmethod
import pandas as pd

class Extractor(ABC):
    @abstractmethod
    def extract(self):
        """Extract data from source (API/CSV/etc.)"""
        pass

class Transformer(ABC):
    @abstractmethod
    def transform(self, data) -> dict[str, pd.DataFrame]:
        """Transform raw data into normalized DataFrames"""
        pass

class Loader(ABC):
    @abstractmethod
    def load(self, data: pd.DataFrame, table_name: str):
        """Load DataFrame into destination (DB)"""
        pass
