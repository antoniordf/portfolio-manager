from .base import DataFetcher
from typing import Dict, Any, List
from datetime import datetime
import requests
import logging

logger = logging.getLogger(__name__)

class PolygonFetcher(DataFetcher):
    """
    Fetcher for Polygon.io API.
    """

    def fetch_metadata(self, series_id: str) -> Dict[str, Any]:
        """
        Fetch metadata for a given ticker from Polygon.io.
        """
        url = f'https://api.polygon.io/v3/reference/tickers/{series_id}'
        params = {
            'apiKey': self.api_key
        }
        try:
            response = self.make_request_with_backoff(url, params=params)
            data = response.json()
            return self.parse_metadata(data)
        except requests.RequestException as e:
            logger.error(f"Error fetching metadata for {series_id} from Polygon.io: {e}")
            raise

    def fetch_series_data(self, series_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        url = f'https://api.polygon.io/v2/aggs/ticker/{series_id}/range/1/day/{start_date}/{end_date}'
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000,
            'apiKey': self.api_key
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return self.parse_series_data(response.json())

    def parse_metadata(self, response_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse the metadata from the Polygon.io response.
        """
        results = response_json.get('results', {})
        if not results:
            logger.error(f"No metadata found for ticker.")
            raise ValueError("No metadata found for ticker.")
        
        return {
            'id': results.get('ticker'),
            'title': results.get('name'),
            'observation_start': results.get('list_date', '2000-01-01'),  # Using list_date as a proxy
            'observation_end': datetime.today().strftime('%Y-%m-%d'),
            'frequency': 'Daily',
            'units': 'USD',
            'seasonal_adjustment': 'N/A',
            'last_updated': datetime.now().isoformat(),
            'notes': results.get('description', ''),
            'data_origin': 'polygon'  
        }

    def parse_series_data(self, response_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        results = response_json.get('results', [])
        parsed_data = []
        for item in results:
            timestamp = item['t'] / 1000  # Convert from milliseconds to seconds
            date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            parsed_data.append({
                'date': date,
                'open': item.get('o', 0),
                'high': item.get('h', 0),
                'low': item.get('l', 0),
                'close': item.get('c', 0),
                'volume': item.get('v', 0)
            })
        return parsed_data