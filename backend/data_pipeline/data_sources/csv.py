from typing import Dict, Any, List
import pandas as pd
from datetime import datetime
from .base import DataFetcher
import logging

logger = logging.getLogger(__name__)

class CSVFetcher(DataFetcher):
    def __init__(self, api_key: str = None, csv_paths: Dict[str, str] = None):
        super().__init__(api_key='')
        self.csv_paths = csv_paths or {}
        # Define date formats for different files
        self.date_formats = {
            'pmi_manufacturing': '%d/%m/%Y',
            'pmi_services': '%d/%m/%Y',
            'vix': '%d/%m/%Y',
            'dxy': '%d/%m/%Y',
            'sp_500': '%m/%d/%Y',
            'nasdaq': '%m/%d/%Y',
            'credit': '%d/%m/%Y',
            'nfib': '%d/%m/%Y',
            'consumer_sentiment': '%d/%m/%Y',
            'wb_commodity_agriculture_index': '%d/%m/%Y',
            'wb_commodity_energy_index': '%d/%m/%Y',
            'wb_commodity_metals_index': '%d/%m/%Y'
        }

    def _get_csv_data(self, series_id: str) -> pd.DataFrame:
        for source, path in self.csv_paths.items():
            try:
                logger.debug(f"Reading {source} from {path}")
                df = pd.read_csv(path)
                
                # Standardize date column name
                df.rename(columns={'Date': 'date'}, inplace=True)
                
                if series_id in df.columns:
                    result_df = df[['date', series_id]].copy()
                    
                    # Parse dates with correct format
                    date_format = self.date_formats.get(source)
                    if date_format:
                        result_df['date'] = pd.to_datetime(
                            result_df['date'], 
                            format=date_format, 
                            errors='coerce'
                        )
                    
                    return result_df.dropna()
                    
            except Exception as e:
                logger.warning(f"Error reading {path}: {e}")
                
        raise ValueError(f"Series {series_id} not found in CSV files")

    def fetch_metadata(self, series_id: str) -> Dict[str, Any]:
        df = self._get_csv_data(series_id)
        return self.parse_metadata({'data': df, 'series_id': series_id})

    def parse_metadata(self, response_json: Dict[str, Any]) -> Dict[str, Any]:
        df = response_json['data']
        series_id = response_json['series_id']
        
        df['date'] = pd.to_datetime(df['date'])
        df = df.dropna()
        
        freq = pd.infer_freq(df['date'])
        frequency = {
            'D': 'Daily',
            'W': 'Weekly',
            'M': 'Monthly',
            'Q': 'Quarterly',
            'A': 'Annual'
        }.get(freq, 'Unknown')

        return {
            'id': series_id,
            'title': series_id,
            'observation_start': df['date'].min().strftime('%Y-%m-%d'),
            'observation_end': df['date'].max().strftime('%Y-%m-%d'),
            'frequency': frequency,
            'units': 'N/A',
            'seasonal_adjustment': 'N/A',
            'last_updated': datetime.now().isoformat(),
            'notes': 'Loaded from CSV file'
        }

    def fetch_series_data(self, series_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        df = self._get_csv_data(series_id)
        df['date'] = pd.to_datetime(df['date'])
        mask = (df['date'] >= start_date) & (df['date'] <= end_date)
        filtered_df = df.loc[mask]
        return self.parse_series_data({'data': filtered_df, 'series_id': series_id})

    def parse_series_data(self, response_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        df = response_json['data']
        series_id = response_json['series_id']
        
        return [
            {
                'date': row['date'].strftime('%Y-%m-%d'),
                'value': float(row[series_id])
            }
            for _, row in df.iterrows()
            if pd.notna(row[series_id])
        ]