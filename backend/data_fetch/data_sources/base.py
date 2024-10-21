from abc import ABC, abstractmethod
from typing import Any, Dict, List
import requests
from dashboard.models import DataSeries, EconomicDataPoint, FinancialDataPoint
from datetime import datetime, timedelta
import logging
import time

logger = logging.getLogger(__name__)

class DataFetcher(ABC):
    """
    Abstract base class for all data fetchers.
    """

    def __init__(self, api_key: str):
        self.api_key = api_key

    @abstractmethod
    def fetch_metadata(self, series_id: str) -> Dict[str, Any]:
        """
        Fetch metadata for a given series ID.
        """
        pass

    @abstractmethod
    def fetch_series_data(self, series_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Fetch series data for a given series ID within a date range.
        """
        pass

    @abstractmethod
    def parse_metadata(self, response_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse the metadata from the API response.
        """
        pass

    @abstractmethod
    def parse_series_data(self, response_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse the series data from the API response.
        """
        pass

    def make_request_with_backoff(self, url: str, params: Dict[str, Any] = None, max_retries: int = 5, backoff_factor: float = 0.3) -> requests.Response:
        """
        Make an HTTP GET request with exponential backoff.
        """
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt == max_retries:
                    logger.error(f"Max retries exceeded for URL: {url}")
                    raise
                sleep_time = backoff_factor * (2 ** (attempt - 1))
                logger.warning(f"Request failed (Attempt {attempt}/{max_retries}): {e}. Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)

    def save_metadata(self, series_id: str, data_type: str, data_origin: str) -> DataSeries:
        """
        Fetch and save metadata for a series.
        """
        try:
            metadata = self.fetch_metadata(series_id)
        except requests.RequestException as e:
            logger.error(f"Error fetching metadata for {series_id}: {e}")
            raise

        data_series_instance, created = DataSeries.objects.update_or_create(
            series_id=metadata['id'],
            defaults={
                'name': metadata['title'],
                'observation_start': metadata['observation_start'],
                'observation_end': metadata['observation_end'],
                'frequency': metadata.get('frequency', 'N/A'),
                'units': metadata.get('units', 'N/A'),
                'seasonal_adjustment': metadata.get('seasonal_adjustment', 'N/A'),
                'last_updated': metadata['last_updated'],
                'notes': metadata.get('notes', ''),
                'data_type': data_type,
                'data_origin': data_origin,
                'metadata': metadata,
            }
        )
        if created:
            logger.info(f"Created new DataSeries: {metadata['id']}")
        else:
            logger.info(f"Updated existing DataSeries: {metadata['id']}")
        return data_series_instance

    def save_series_data(self, data_series_instance: DataSeries, initial_fetch: bool = False):
        """
        Fetch and save series data for a DataSeries instance.
        
        Args:
            data_series_instance (DataSeries): The DataSeries instance.
            initial_fetch (bool): If True, fetch a limited historical range (e.g., last 5 years).
        """
        data_type = data_series_instance.data_type
        if data_type == 'economic':
            data_point_model = EconomicDataPoint
        elif data_type == 'financial':
            data_point_model = FinancialDataPoint
        else:
            logger.error(f"Unknown data_type: {data_type} for series_id: {data_series_instance.series_id}")
            raise ValueError(f"Unknown data_type: {data_type}")

        if initial_fetch:
            # Define the start_date as 5 years ago from today
            start_date = (datetime.today() - timedelta(days=5*365)).strftime('%Y-%m-%d')
            logger.info(f"Initial fetch for series {data_series_instance.series_id} starting from {start_date}")
        else:
            # Determine the last date of existing data
            last_data_point = data_point_model.objects.filter(series=data_series_instance).order_by('-date').first()
            if last_data_point:
                last_date = last_data_point.date
                start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"Fetching new data for series {data_series_instance.series_id} starting from {start_date}")
            else:
                # Use observation_start or a default date
                if data_series_instance.observation_start:
                    start_date = data_series_instance.observation_start.strftime('%Y-%m-%d')
                    logger.info(f"No existing data found. Fetching from observation_start {start_date}")
                else:
                    start_date = '2000-01-01'
                    logger.info(f"No observation_start found. Fetching from default date {start_date}")

        end_date = datetime.today().strftime('%Y-%m-%d')
        try:
            series_data = self.fetch_series_data(data_series_instance.series_id, start_date, end_date)
            observations = self.parse_series_data(series_data)
        except Exception as e:
            logger.error(f"Failed to parse series data for {data_series_instance.series_id}: {e}")
            raise  # Re-raise the exception to be handled by the calling task

        # Prepare data points
        data_points_to_create = []
        for obs in observations:
            data_points_to_create.append(
                data_point_model(
                    series=data_series_instance,
                    date=obs['date'],
                    open=obs.get('open'),
                    high=obs.get('high'),
                    low=obs.get('low'),
                    close=obs.get('close'),
                    volume=obs.get('volume')
                )
            )

        # Bulk create to minimize database hits
        if data_points_to_create:
            try:
                data_point_model.objects.bulk_create(data_points_to_create, ignore_conflicts=True)
                logger.info(f"Saved {len(data_points_to_create)} data points for {data_series_instance.series_id}")
            except Exception as e:
                logger.error(f"Error bulk creating data points for {data_series_instance.series_id}: {e}")
                raise  # Re-raise to ensure the task is aware of the failure
        else:
            logger.info(f"No new data points to save for {data_series_instance.series_id}")