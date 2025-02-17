from typing import Dict, Any, List
import pandas as pd
from datetime import datetime
from .base import DataFetcher
import logging

logger = logging.getLogger(__name__)

class CSVFetcher(DataFetcher):
    """
    CSVFetcher handles loading data from CSV files.
    
    For financial data, the CSV file is assumed to be dedicated to a single series,
    and its key (in csv_paths) is used as the series_id. For economic data, the CSV file 
    may contain many columns (one for each series), so we search for the given series_id 
    among the columns.
    """
    def __init__(self, api_key: str = None, csv_paths: Dict[str, str] = None):
        super().__init__(api_key=api_key or '')
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

    def _get_csv_data(self, series_id: str, data_type: str) -> pd.DataFrame:
        """
        Returns a DataFrame for the requested series.
        
        - For financial data, it expects a CSV file keyed exactly by series_id.
        - For economic data, it iterates through all CSV files and returns the one
          that contains the series_id as a column.
        """
        if data_type == 'financial':
            # For financial data, the CSV file is dedicated to that series.
            if series_id in self.csv_paths:
                path = self.csv_paths[series_id]
                try:
                    df = pd.read_csv(path)
                    # Rename 'Date' to 'date' if needed.
                    if 'Date' in df.columns:
                        df.rename(columns={'Date': 'date'}, inplace=True)
                    return df
                except Exception as e:
                    logger.error(f"Error reading CSV for financial series {series_id} from {path}: {e}")
                    raise
            else:
                raise ValueError(f"CSV file for financial series '{series_id}' not found in csv_paths.")
        else:  # economic
            # Loop over all CSV files
            for source, path in self.csv_paths.items():
                try:
                    df = pd.read_csv(path)
                    if 'Date' in df.columns:
                        df.rename(columns={'Date': 'date'}, inplace=True)
                    if series_id in df.columns:
                        return df[['date', series_id]].copy()
                    # If there are exactly two columns and one is date, assume the other is the data.
                    if len(df.columns) == 2 and 'date' in df.columns:
                        data_col = [col for col in df.columns if col != 'date'][0]
                        df = df.copy()
                        df.rename(columns={data_col: series_id}, inplace=True)
                        return df[['date', series_id]]
                except Exception as e:
                    logger.warning(f"Error reading CSV from {path}: {e}")
            raise ValueError(f"Series {series_id} not found in any CSV file.")

    def fetch_metadata(self, series_id: str, data_type: str = 'economic') -> Dict[str, Any]:
        df = self._get_csv_data(series_id, data_type)
        return self.parse_metadata({'data': df, 'series_id': series_id}, data_type)

    def parse_metadata(self, response_json: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        df = response_json['data']
        series_id = response_json['series_id']
        
        df['date'] = pd.to_datetime(df['date'])
        df = df.dropna()
        
        if data_type == 'financial':
            frequency = 'Daily'
        else:
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

    def fetch_series_data(self, series_id: str, start_date: str, end_date: str, data_type: str = 'economic') -> List[Dict[str, Any]]:
        """
        Fetch time series data from CSV for the given series_id within the date range.
        Returns a list of dictionaries.
        """
        df = self._get_csv_data(series_id, data_type)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        mask = (df['date'] >= start_date) & (df['date'] <= end_date)
        filtered_df = df.loc[mask]
        return self.parse_series_data({'data': filtered_df, 'series_id': series_id}, data_type)

    def parse_series_data(self, response_json: Dict[str, Any], data_type: str = 'economic') -> List[Dict[str, Any]]:
        """
        For economic data, assume the DataFrame contains 'date' and the series column.
        For financial data, assume the DataFrame contains standard columns.
        """
        df = response_json['data']
        series_id = response_json['series_id']
        if data_type == 'financial':
            # For financial CSVs, return rows with expected columns.
            required = {'date', 'open', 'high', 'low', 'close', 'volume'}
            if not required.issubset(df.columns):
                raise ValueError(f"Financial CSV missing required columns. Found: {df.columns.tolist()}")
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])
            return [
                {
                    'date': row['date'].strftime('%Y-%m-%d'),
                    'open': float(row['open']) if pd.notna(row['open']) else None,
                    'high': float(row['high']) if pd.notna(row['high']) else None,
                    'low': float(row['low']) if pd.notna(row['low']) else None,
                    'close': float(row['close']) if pd.notna(row['close']) else None,
                    'volume': int(row['volume']) if pd.notna(row['volume']) else None,
                }
                for _, row in df.iterrows()
            ]
        else:  # economic
            # For economic CSVs, assume the DataFrame has two columns: 'date' and series_id.
            if series_id not in df.columns:
                raise ValueError(f"Series {series_id} not found in CSV data.")
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date', series_id])
            return [
                {
                    'date': row['date'].strftime('%Y-%m-%d'),
                    'value': float(row[series_id])
                }
                for _, row in df.iterrows()
            ]