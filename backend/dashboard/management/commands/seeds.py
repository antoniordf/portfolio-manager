from django.conf import settings
from django.core.management.base import BaseCommand
from typing import List, Dict
from data_pipeline.flows import fetch_and_store_flow
import pandas as pd
from google.cloud import bigquery
from data_pipeline.data_sources.fred import FREDFetcher
from data_pipeline.utils import load_historical_economic_data_to_bq
from fredapi import Fred

fred_api_key = settings.FRED_API_KEY
fred = Fred(api_key=fred_api_key)
fred_fetcher = FREDFetcher(fred_api_key)

class Command(BaseCommand):
    help = 'Seeds the database with initial macro and financial data.'

    fred_series_ids = {
    'gdp': 'GDPC1',
    'cpi': 'CPIAUCSL',
    'cpi_core': 'CPILFESL',
    'treasury_yield_10y': 'DGS10',
    'treasury_yield_5y': 'DGS5',
    'treasury_yield_2y': 'DGS2',
    'treasury_yield_1y': 'DGS1',
    '5yr_tips': 'DFII5',
    '7yr_tips': 'DFII7',
    '10yr_tips': 'DFII10',
    'housing_permits_index': 'PERMIT',
    'housing_starts_index': 'HOUST',
    'housing_completions_index': 'COMPUTSA',
    'industrial_production': 'INDPRO',
    'nonfarm_payroll': 'PAYEMS',
    'unemployment_rate': 'UNRATE',
    'm2_money_supply': 'M2SL',
    'consumer_sentiment': 'UMCSENT',
    'continued_claims': 'CCSA',
    'durable_goods_new_orders': 'DGORDER',
    'financial_conditions_index': 'NFCI',
    'inflation_expectation': 'MICH',
    'initial_claims': 'ICSA',
    'new_orders_non_durable_goods': 'AMNMNO',
    'ppi_commodities': 'PPIACO',
    'ppi_finished_goods': 'WPSFD49207',
    'retail_sales': 'MRTSSM44000USS',
    'consumer_credit': 'TOTALSL'
}
    
    # BigQuery table IDs
    FINANCIAL_TABLE_ID = "portfolio-manager-445317.market_data.financial_data_points"
    ECONOMIC_TABLE_ID  = "portfolio-manager-445317.market_data.economic_data_points"

    def table_is_empty(self, table_id: str) -> bool:
        """
        Returns True if the BigQuery table has zero rows, False otherwise.
        """
        client = bigquery.Client()
        query = f"SELECT COUNT(*) as cnt FROM `{table_id}`"
        result = client.query(query).result()
        row = next(iter(result), None)
        return (row.cnt == 0)
    
    def fetch_fred_series(self, series_id, start_date=None, end_date=None):
        """
        Fetch a single data series from the FRED API.

        Parameters:
        - series_id (str): The FRED series ID (e.g., 'GDPC1', 'CPIAUCSL').
        - start_date (str): Optional start date for the data (e.g., '2000-01-01').
        - end_date (str): Optional end date for the data (e.g., '2023-01-01').

        Returns:
        - pd.DataFrame: A DataFrame with 'Date' and 'value' columns.
        """
        data = fred.get_series(series_id, start=start_date, end=end_date)
        df = data.reset_index()
        df.columns = ['Date', 'value']
        return df
    
    def prepare_data(self, fred_series_ids):
        # Scrape S&P 500 data from Wikipedia
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        sp500_table = pd.read_html(url)[0]
        sp_500_tickers = list(sp500_table['Symbol'])

        data_sp500 = [{
                'series_id': ticker,
                'data_origin': 'polygon',
                'data_type': 'financial',
            } for ticker in sp_500_tickers]
        
        data_fred = [{
            'series_id': series_id,
            'data_origin': 'fred',
            'data_type': 'economic',
        } for series_id in fred_series_ids.values()]
        
        return data_sp500, data_fred

    def handle(self, *args, **kwargs):
        data_sp500, data_fred = self.prepare_data(self.fred_series_ids)
        total_series_sp500 = len(data_sp500)
        self.stdout.write(f"Preparing to seed {total_series_sp500} data series...")

        BATCH_SIZE = 50
        for i in range(0, len(data_sp500), BATCH_SIZE):
            batch = data_sp500[i : i + BATCH_SIZE]
            try:
                # This runs your Prefect flow in the main process, sequentially
                fetch_and_store_flow(batch)  
                self.stdout.write(self.style.SUCCESS(
                    f"Processed batch from index {i} to {i + BATCH_SIZE}"
                ))
            except Exception as e:
                self.stderr.write(self.style.ERROR(f"Error triggering data fetching tasks: {e}"))
                break
        
        if self.table_is_empty(self.ECONOMIC_TABLE_ID):
            self.stdout.write("Loading economic data to BigQuery via csv...")

            historical_rows = []

            # Save Metadata for FRED series
            for item in data_fred:
                series_id = item['series_id']
                data_type = item['data_type']
                data_origin = item['data_origin']

                # 1) Save metadata => create DataSeries
                try:
                    fred_fetcher.save_metadata(series_id, data_type, data_origin)
                    self.stdout.write(self.style.SUCCESS(f"Saved metadata for series: {series_id}"))
                except Exception as e:
                    self.stderr.write(self.style.ERROR(f"Failed to save metadata for {series_id}: {e}"))
                    continue  # Skip to the next series if metadata saving fails

                # 2) Fetch entire range
                try:
                    observations = self.fetch_fred_series(series_id)
                    self.stdout.write(self.style.SUCCESS(f"Fetched data for series: {series_id}"))
                except Exception as e:
                    self.stderr.write(self.style.ERROR(f"Failed to fetch data for {series_id}: {e}"))
                    continue  # Skip to the next series if data fetching fails
                
                # 3) Transform into { series_id, date, value } and accumulate
                for _, row in observations.iterrows():
                    historical_rows.append({
                        "series_id": series_id,
                        "date": row["Date"].strftime('%Y-%m-%d') if isinstance(row["Date"], pd.Timestamp) else row["Date"],
                        "value": row["value"],
                    })

             # 4) Bulk load into BQ once
            try:
                load_historical_economic_data_to_bq(self.ECONOMIC_TABLE_ID, historical_rows)
                self.stdout.write(self.style.SUCCESS(
                    f"Loaded {len(historical_rows)} older FRED rows into {self.ECONOMIC_TABLE_ID}"
                ))
            except Exception as e:
                self.stderr.write(self.style.ERROR(f"Failed to load data into BigQuery: {e}"))
        else:
            for i in range(0, len(data_fred), BATCH_SIZE):
                batch = data_fred[i : i + BATCH_SIZE]
                try:
                    # This runs your Prefect flow in the main process, sequentially
                    fetch_and_store_flow(batch)  
                    self.stdout.write(self.style.SUCCESS(
                        f"Processed batch from index {i} to {i + BATCH_SIZE}"
                    ))
                except Exception as e:
                    self.stderr.write(self.style.ERROR(f"Error triggering data fetching tasks: {e}"))
                    break