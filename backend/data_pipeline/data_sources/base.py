from abc import ABC, abstractmethod
from typing import Any, Dict, List
import requests
from dashboard.models import DataSeries
from datetime import datetime, timedelta
import logging
import time
from google.cloud import bigquery
import os

logger = logging.getLogger(__name__)

# Set up BigQuery client
client = bigquery.Client()

# Configure your BigQuery tables
ECONOMIC_TABLE_ID = "portfolio-manager-445317.market_data.economic_data_points"
FINANCIAL_TABLE_ID = "portfolio-manager-445317.market_data.financial_data_points"

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

    def make_request_with_backoff(
            self, url: str, 
            params: Dict[str, Any] = None, 
            max_retries: int = 5, 
            backoff_factor: float = 0.3
        ) -> requests.Response:
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
        
        if initial_fetch:
            # Define the start_date as 5 years ago from today
            start_date = (datetime.today() - timedelta(days=5*365)).strftime('%Y-%m-%d')
            logger.info(f"Initial fetch for series {data_series_instance.series_id} starting from {start_date}")
        else:
             # Let's say we store last_fetched_date in metadata to track progress:
            last_fetched_date = data_series_instance.metadata.get('last_fetched_date', None)
            if last_fetched_date:
                start_dt = datetime.strptime(last_fetched_date, '%Y-%m-%d')
                start_date = (start_dt + timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"Fetching new data for series {data_series_instance.series_id} starting from {start_date}")
            else:
                # No last_fetched_date: default to initial fetch behavior
                start_date = (datetime.today() - timedelta(days=5*365)).strftime('%Y-%m-%d')
                logger.info(f"No existing data found in BigQuery. Starting from {start_date}")

        end_date = datetime.today().strftime('%Y-%m-%d')

        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        if start_dt > end_dt:
            logger.warning(f"Start date {start_date} is after end date {end_date} for series {data_series_instance.series_id}. \
                           The series is already up to date. Skipping fetch.")
            return
    
        try:
            observations = self.fetch_series_data(data_series_instance.series_id, start_date, end_date)
        except Exception as e:
            logger.error(f"Failed to fetch or parse series data for {data_series_instance.series_id}: {e}")
            raise  # Re-raise the exception to be handled by the Celery task

        # Insert into BigQuery
        if data_type == 'financial':
            table_id = FINANCIAL_TABLE_ID
            rows_to_insert = []
            for obs in observations:
                # obs: { 'date':..., 'open':..., 'high':..., 'low':..., 'close':..., 'volume':... }
                rows_to_insert.append({
                    "series_id": data_series_instance.series_id,
                    "date": obs['date'],
                    "open": obs.get('open'),
                    "high": obs.get('high'),
                    "low": obs.get('low'),
                    "close": obs.get('close'),
                    "volume": obs.get('volume'),
                })
        elif data_type == 'economic':
            table_id = ECONOMIC_TABLE_ID
            rows_to_insert = []
            for obs in observations:
                # obs: { 'date':..., 'value':... }
                rows_to_insert.append({
                    "series_id": data_series_instance.series_id,
                    "date": obs['date'],
                    "value": obs.get('value'),
                })
        else:
            logger.error(f"Unknown data_type: {data_type} for series {data_series_instance.series_id}")
            raise ValueError(f"Unknown data_type: {data_type}")

        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            logger.error(f"Error inserting rows into BigQuery for {data_series_instance.series_id}: {errors}")
            raise ValueError(f"BigQuery insert errors: {errors}")
        else:
            logger.info(f"Inserted {len(rows_to_insert)} rows into BigQuery for {data_series_instance.series_id}.")

            # Update last_fetched_date in metadata
            # Assuming observations are sorted by date, take the max date as last fetched:
            latest_date = max(obs['date'] for obs in observations)
            metadata = data_series_instance.metadata
            metadata['last_fetched_date'] = latest_date
            data_series_instance.metadata = metadata
            data_series_instance.save()

            # Update last_updated
            data_series_instance.last_updated = datetime.utcnow()
            data_series_instance.save()