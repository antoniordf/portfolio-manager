from abc import ABC, abstractmethod
from typing import Any, Dict, List
import requests
from django.conf import settings
from dashboard.models import DataSeries, EconomicDataPoint, FinancialDataPoint
from datetime import datetime, timedelta
import logging

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

    def save_series_data(self, data_series_instance: DataSeries):
        """
        Fetch and save series data for a DataSeries instance.
        """
        data_type = data_series_instance.data_type
        if data_type == 'economic':
            data_point_model = EconomicDataPoint
        elif data_type == 'financial':
            data_point_model = FinancialDataPoint
        else:
            logger.error(f"Unknown data_type: {data_type} for series_id: {data_series_instance.series_id}")
            raise ValueError(f"Unknown data_type: {data_type}")

        # Determine the last date of existing data
        last_data_point = data_point_model.objects.filter(series=data_series_instance).order_by('-date').first()
        if last_data_point:
            last_date = last_data_point.date
            start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            # Use observation_start or a default date
            if data_series_instance.observation_start:
                start_date = data_series_instance.observation_start.strftime('%Y-%m-%d')
            else:
                start_date = '2000-01-01'

        end_date = datetime.today().strftime('%Y-%m-%d')
        try:
            series_data = self.fetch_series_data(data_series_instance.series_id, start_date, end_date)
            observations = self.parse_series_data(series_data)
        except requests.RequestException as e:
            logger.error(f"Error fetching series data for {data_series_instance.series_id}: {e}")
            raise

        # Prepare data points
        data_points_to_create = []
        for obs in observations:
            date_str = obs['date']
            try:
                date = datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError as e:
                logger.error(f"Invalid date format for series {data_series_instance.series_id}: {date_str}")
                continue

            if data_type == 'economic':
                value_str = obs.get('value', '')
                if value_str in ('', None):
                    continue
                try:
                    value = float(value_str)
                except ValueError:
                    logger.warning(f"Non-numeric value for series {data_series_instance.series_id} on {date}: {value_str}")
                    continue
                data_points_to_create.append(
                    EconomicDataPoint(
                        series=data_series_instance,
                        date=date,
                        value=value
                    )
                )
            elif data_type == 'financial':
                try:
                    open_price = float(obs.get('open', 0))
                    high_price = float(obs.get('high', 0))
                    low_price = float(obs.get('low', 0))
                    close_price = float(obs.get('close', 0))
                    volume = int(obs.get('volume', 0))
                except (ValueError, TypeError) as e:
                    logger.error(f"Error parsing financial data for {data_series_instance.series_id} on {date}: {e}")
                    continue
                data_points_to_create.append(
                    FinancialDataPoint(
                        series=data_series_instance,
                        date=date,
                        open=open_price,
                        high=high_price,
                        low=low_price,
                        close=close_price,
                        volume=volume
                    )
                )

        # Bulk create to minimize database hits
        if data_points_to_create:
            try:
                data_point_model.objects.bulk_create(data_points_to_create, ignore_conflicts=True)
                logger.info(f"Saved {len(data_points_to_create)} data points for {data_series_instance.series_id}")
            except Exception as e:
                logger.error(f"Error bulk creating data points for {data_series_instance.series_id}: {e}")
        else:
            logger.info(f"No new data points to save for {data_series_instance.series_id}")