from celery import shared_task
from django.conf import settings
from .data_sources.fetcher_manager import FetcherManager
from dashboard.models import DataSeries
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

def get_api_key(data_origin: str) -> str:
    """
    Retrieve the API key based on data origin.

    Args:
        data_origin (str): The data origin ('fred' or 'polygon').

    Returns:
        str: The corresponding API key.

    Raises:
        ValueError: If no API key is found for the given data origin.
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

@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_and_store_multiple_series(self, data_series_list: List[Dict[str, str]]):
    """
    Celery task to fetch and store metadata and series data for multiple DataSeries.

    Args:
        data_series_list (List[Dict[str, str]]): A list of dictionaries, each containing:
            - 'series_id': The unique identifier for the data series (e.g., ticker symbol).
            - 'data_origin': The source of the data (e.g., 'fred', 'polygon').
            - 'data_type': The type of data ('economic' or 'financial').
    """
    for item in data_series_list:
        series_id = item.get('series_id')
        data_origin = item.get('data_origin')
        data_type = item.get('data_type')

        if not all([series_id, data_origin, data_type]):
            logger.error(f"Missing data for series: {item}")
            continue

        try:
            # Fetch API key based on data_origin
            api_key = get_api_key(data_origin)

            # Check if the series already exists
            try:
                series_instance = DataSeries.objects.get(series_id=series_id)
                logger.info(f"Processing existing series: {series_id}")
                fetcher = FetcherManager.get_fetcher(data_origin, api_key)
                fetcher.save_series_data(series_instance)
                logger.info(f"Successfully updated series: {series_id}")
            except DataSeries.DoesNotExist:
                logger.info(f"Processing new series: {series_id}")
                fetcher = FetcherManager.get_fetcher(data_origin, api_key)
                # Save metadata and create DataSeries instance
                series_instance = fetcher.save_metadata(series_id, data_type, data_origin)
                # Fetch and save series data with initial_fetch=True
                fetcher.save_series_data(series_instance, initial_fetch=True)
                logger.info(f"Successfully created and populated series: {series_id}")

        except Exception as e:
            logger.error(f"Failed to process series {series_id}: {e}")
            # Retry the task for transient errors
            try:
                self.retry(exc=e)
            except self.MaxRetriesExceededError:
                logger.error(f"Max retries exceeded for series {series_id}")
            continue