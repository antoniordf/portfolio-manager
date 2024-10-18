from .base import DataFetcher
from typing import Dict, Any, List

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
        observations = response_json.get('observations', [])
        return [{'date': obs['date'], 'value': obs['value']} for obs in observations]