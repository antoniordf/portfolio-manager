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
        try:
            response = self.make_request_with_backoff(url, params=params)
            return self.parse_series_data(response.json())
        except requests.RequestException as e:
            logger.error(f"Error fetching series data for {series_id}: {e}")
            raise

    def parse_metadata(self, response_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse the metadata from the Polygon.io response.
        """
        if 'results' not in response_json:
            logger.error(f"No 'results' key found in response: {response_json}")
            raise ValueError("Invalid response format: 'results' key missing.")
        
        results = response_json['results']
        
        return {
            'id': results.get('ticker'),
            'title': results.get('name'),
            'observation_start': results.get('list_date', '2000-01-01'),  # Using list_date as a proxy
            'observation_end': datetime.today().strftime('%Y-%m-%d'),
            'frequency': 'Daily',
            'units': results.get('currency_name'),
            'seasonal_adjustment': 'N/A',
            'last_updated': datetime.now().isoformat(),
            'notes': results.get('description', ''), 
        }

    def parse_series_data(self, response_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        results = response_json.get('results', [])
        if not isinstance(results, list):
            logger.error(f"Unexpected data format for series data: {response_json}")
            raise ValueError("Unexpected data format for series data.")
        
        parsed_data = []
        required_keys = ['t', 'o', 'h', 'l', 'c', 'v']
        
        for item in results:
            # Check if all required keys are present in the item
            if not all(key in item for key in required_keys):
                logger.error(f"Missing required keys in data item: {item}")
                raise ValueError("Missing required keys in data item.")
            
            # Safely extract and convert data
            try:
                timestamp = item['t'] / 1000  # Convert from milliseconds to seconds
                date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            except (KeyError, TypeError, ValueError) as e:
                logger.error(f"Error processing timestamp in item {item}: {e}")
                raise ValueError(f"Invalid timestamp in data item: {item}") from e
            
            # Safely extract financial data with defaults
            open_price = item.get('o', None)
            high_price = item.get('h', None)
            low_price = item.get('l', None)
            close_price = item.get('c', None)
            volume = item.get('v', None)
            
            # Validate financial data
            if None in (open_price, high_price, low_price, close_price, volume):
                logger.error(f"Missing financial data in item: {item}")
                raise ValueError(f"Missing financial data in data item: {item}")
            
            # Append the parsed data
            parsed_data.append({
                'date': date,
                'open': float(open_price),
                'high': float(high_price),
                'low': float(low_price),
                'close': float(close_price),
                'volume': int(volume)
            })
        
        return parsed_data