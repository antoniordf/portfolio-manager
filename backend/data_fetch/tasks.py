from celery import shared_task
from django.conf import settings
from .data_sources.fetcher_manager import FetcherManager
from dashboard.models import DataSeries
from typing import List, Dict
from django.db import transaction
import logging

logger = logging.getLogger(__name__)

@shared_task
def fetch_and_store_multiple_series(data_series_list: List[Dict[str, str]]):
    """
    Celery task to fetch and store metadata and series data for multiple DataSeries.
    
    Args:
        data_series_list (List[Dict[str, str]]): A list of dictionaries, each containing:
            - 'series_id': The unique identifier for the data series (e.g., ticker symbol).
            - 'data_origin': The source of the data (e.g., 'fred', 'polygon').
            - 'data_type': The type of data ('economic' or 'financial').
    """
    # Extract series_ids from the list
    series_ids = [item['series_id'] for item in data_series_list]
    
    # Fetch existing DataSeries from the database
    existing_series = DataSeries.objects.filter(series_id__in=series_ids)
    existing_series_dict = {series.series_id: series for series in existing_series}
    
    # Determine which series are new
    new_series = [item for item in data_series_list if item['series_id'] not in existing_series_dict]
    
    # Process existing series
    for series in existing_series:
        try:
            fetcher = FetcherManager.get_fetcher(series.data_origin, get_api_key(series.data_origin))
            fetcher.save_series_data(series)
        except Exception as e:
            logger.error(f"Failed to fetch and save data for existing series {series.series_id}: {e}")
            continue
    
    # Process new series
    if new_series:
        # Prepare metadata fetching
        for item in new_series:
            series_id = item['series_id']
            data_origin = item['data_origin']
            data_type = item['data_type']
            try:
                fetcher = FetcherManager.get_fetcher(data_origin, get_api_key(data_origin))
                data_series_instance = fetcher.save_metadata(series_id, data_type, data_origin)
                fetcher.save_series_data(data_series_instance)
            except Exception as e:
                logger.error(f"Failed to fetch and save data for new series {series_id}: {e}")
                continue

def get_api_key(data_origin: str) -> str:
    """
    Retrieve the API key based on data origin.
    """
    api_keys = {
        'fred': settings.FRED_API_KEY,
        'polygon': settings.POLYGON_API_KEY,
        # Add other data sources and their keys here
    }
    api_key = api_keys.get(data_origin.lower())
    if not api_key:
        raise ValueError(f"No API key found for data origin: {data_origin}")
    return api_key