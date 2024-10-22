from typing import Type
from .fred import FREDFetcher
from .polygon import PolygonFetcher

class FetcherManager:
    """
    Manager to handle different data fetchers based on data origin.
    """

    FETCHERS = {
        'fred': FREDFetcher,
        'polygon': PolygonFetcher,
        # Add other data sources here
    }

    @classmethod
    def get_fetcher(cls, data_origin: str, api_key: str):
        fetcher_class = cls.FETCHERS.get(data_origin.lower())
        if not fetcher_class:
            raise ValueError(f"No fetcher available for data origin: {data_origin}")
        return fetcher_class(api_key)