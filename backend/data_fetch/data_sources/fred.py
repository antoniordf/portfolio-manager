from .base import DataFetcher
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class FREDFetcher(DataFetcher):
    """
    Fetcher for FRED (Federal Reserve Economic Data) API.
    """

    def fetch_metadata(self, series_id: str) -> Dict[str, Any]:
        url = f'https://api.stlouisfed.org/fred/series?series_id={series_id}&api_key={self.api_key}&file_type=json'
        response = self.make_request_with_backoff(url)
        return self.parse_metadata(response.json())

    def fetch_series_data(self, series_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        url = f'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={self.api_key}&file_type=json&observation_start={start_date}&observation_end={end_date}'
        response = self.make_request_with_backoff(url)
        return self.parse_series_data(response.json())

    def parse_metadata(self, response_json: Dict[str, Any]) -> Dict[str, Any]:
        series = response_json['seriess'][0]
        return {
            'id': series['id'],
            'title': series['title'],
            'observation_start': series['observation_start'],
            'observation_end': series['observation_end'],
            'frequency': series.get('frequency', 'N/A'),
            'units': series.get('units', 'N/A'),
            'seasonal_adjustment': series.get('seasonal_adjustment', 'N/A'),
            'last_updated': series['last_updated'],
            'notes': series.get('notes', '')
        }

    def parse_series_data(self, response_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(response_json, dict):
            logger.error(f"Unexpected response type: {type(response_json).__name__}. Response content: {response_json}")
            raise ValueError("Expected response_json to be a dictionary.")
        
        observations = response_json.get('observations', [])
        if not isinstance(observations, list):
            logger.error(f"Unexpected 'observations' type: {type(observations).__name__}. Response content: {response_json}")
            raise ValueError("Expected 'observations' to be a list.")
        
        parsed_data = []
        required_keys = ['date', 'value']
        
        for obs in observations:
            if not all(key in obs for key in required_keys):
                logger.error(f"Missing required keys in observation: {obs}")
                raise ValueError("Missing required keys in observation.")
            
            date_str = obs['date']
            value_str = obs['value']
            
            if value_str in ('', None):
                logger.error(f"Missing value for observation on {date_str}: {obs}")
                raise ValueError("Missing value in observation.")
            
            try:
                value = float(value_str)
            except ValueError as e:
                logger.error(f"Invalid value for observation on {date_str}: {value_str}")
                raise ValueError("Invalid value in observation.") from e
            
            parsed_data.append({
                'date': date_str,
                'value': value
            })
        
        return parsed_data