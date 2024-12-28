import logging
from typing import List, Dict
from prefect import task, get_run_logger
from django.conf import settings
from dashboard.models import DataSeries
from data_pipeline.data_sources.fetcher_manager import FetcherManager

@task
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

@task
def fetch_and_store_single_series(item: Dict[str, str]):
    """
    Prefect task to fetch and store metadata and series data for a single DataSeries.
    """
    logger = get_run_logger()

    series_id = item.get('series_id')
    data_origin = item.get('data_origin')
    data_type = item.get('data_type')

    if not all([series_id, data_origin, data_type]):
        logger.error(f"Missing data for series: {item}")
        return

    # Fetch API key
    try:
        api_key = get_api_key(data_origin)
    except ValueError as e:
        logger.error(f"API key retrieval failed for data origin '{data_origin}': {e}")
        return

    # Attempt to fetch and store data
    try:
        series_instance = DataSeries.objects.get(series_id=series_id)
        logger.info(f"Processing existing series: {series_id}")
        logger.info(f"data_origin: {series_instance.data_origin}, data_type: {series_instance.data_type}")
        fetcher = FetcherManager.get_fetcher(data_origin, api_key)
        fetcher.save_series_data(series_instance)
        logger.info(f"Successfully updated series: {series_id}")
    except DataSeries.DoesNotExist:
        try:
            logger.info(f"Processing new series: {series_id}")
            fetcher = FetcherManager.get_fetcher(data_origin, api_key)
            series_instance = fetcher.save_metadata(series_id, data_type, data_origin)
            fetcher.save_series_data(series_instance, initial_fetch=True)
            logger.info(f"Successfully created and populated series: {series_id}")
        except Exception as e:
            logger.error(f"Failed to process new series {series_id}: {e}")
            # Prefect retries are handled by the @task decorator, no manual self.retry needed
            raise e
    except Exception as e:
        logger.error(f"Failed to update existing series {series_id}: {e}")
        raise e

@task
def fetch_and_store_multiple_series(data_series_list: List[Dict[str, str]]):
    """
    Prefect task to fetch and store multiple DataSeries.
    """
    logger = get_run_logger()
    logger.info(f"Fetching {len(data_series_list)} series, one by one, sequentially.")
    for item in data_series_list:
        fetch_and_store_single_series(item)
    logger.info("All items processed.")