from datetime import datetime
import os
from django.conf import settings
from django.core.management.base import BaseCommand
from data_pipeline.data_sources.fetcher_manager import FetcherManager
from data_pipeline.flows import fetch_and_store_flow
import pandas as pd
from google.cloud import bigquery
from data_pipeline.data_sources.fred import FREDFetcher
from data_pipeline.utils.utils import load_csv_to_bq, load_csv_in_chunks_to_bq
from fredapi import Fred
from dashboard.models import DataSeries
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    # Get directory where seeds.py is located
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Navigate to data directory
    data_path = os.path.join(current_dir, "..", "..", "data")
    
    csv_file_paths = {
        'pmi_manufacturing': os.path.join(data_path, "pmi_manufacturing.csv"),
        'pmi_services': os.path.join(data_path, "pmi_services.csv"),
        'vix': os.path.join(data_path, "vix.csv"),
        'dxy': os.path.join(data_path, "dxy.csv"),
        'wb_commodity_agriculture_index': os.path.join(data_path, "wb_commodity_agriculture_index.csv"),
        'wb_commodity_energy_index': os.path.join(data_path, "wb_commodity_energy_index.csv"),
        'wb_commodity_metals_index': os.path.join(data_path, "wb_commodity_metals_index.csv"),
        'sp_500': os.path.join(data_path, "sp_500.csv"),
        'nasdaq': os.path.join(data_path, "nasdaq.csv"),
        'credit': os.path.join(data_path, "credit.csv"),
        'nfib': os.path.join(data_path, "nfib.csv"),
        'consumer_sentiment': os.path.join(data_path, "consumer_sentiment.csv")
    }

    date_formats = {
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
    
    # BigQuery table IDs
    FINANCIAL_TABLE_ID = settings.FINANCIAL_TABLE_ID
    ECONOMIC_TABLE_ID  = settings.ECONOMIC_TABLE_ID

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
        if series_id == "UMCSENT" and (start_date is None or start_date < "1978-01-01"):
            start_date = "1978-01-01"

        data = fred.get_series(series_id, start=start_date, end=end_date)
        df = data.reset_index()
        df.columns = ['Date', 'value']
        return df
    
    def process_csv_data(self, csv_file_paths):
        # Define schemas
        economic_schema = [
            bigquery.SchemaField("series_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("component", "STRING", mode="NULLABLE"),  # Allow nulls for non-subcomponent data
            bigquery.SchemaField("value", "FLOAT64", mode="NULLABLE"),
        ]
        financial_schema = [
            bigquery.SchemaField("series_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("open", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("high", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("low", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("close", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("volume", "INTEGER", mode="NULLABLE"),
        ]
        financial_sources = {"vix", "dxy", "sp_500", "nasdaq"}

        csv_fetcher = FetcherManager.get_fetcher('csv', api_key='', csv_paths=self.csv_file_paths)

        for source, path in csv_file_paths.items():
            print(f"\nProcessing {source}: {path}")
            try:
                df = pd.read_csv(path)
            except Exception as e:
                print(f"Error reading {path}: {e}")
                continue

            # Standardize and parse dates
            if 'Date' in df.columns:
                df.rename(columns={'Date': 'date'}, inplace=True)
            elif 'date' not in df.columns:
                print(f"No date column found in {path}.")
                continue
            
            # Determine date format for this source
            date_format = self.date_formats.get(source, '%d/%m/%Y')
            df['date'] = pd.to_datetime(df['date'], format=date_format, errors='coerce')
            df.sort_values(by='date', inplace=True)
            df = df.dropna(subset=['date'])

            # Define data type based on source
            data_type = 'financial' if source in financial_sources else 'economic'

            # Create an instance of DataSeries to contain the series metadata
            try:
                ds_instance = csv_fetcher.save_metadata(source, data_type, 'csv_file')
            except Exception as e:
                print(f"Error saving metadata for {source}: {e}")
                # Depending on needs, continue or handle error
                continue

            # Use the DataSeries instance created from the file key
            main_series = ds_instance.series_id  # e.g. "pmi_manufacturing"
            last_data_main = ds_instance.last_data_date

            if data_type == 'financial':
                # Process financial CSV: assume columns include date, open, high, low, close, volume.
                required_cols = {"open", "high", "low", "close", "volume"}
                if not required_cols.issubset(df.columns):
                    print(f"Missing one of required columns {required_cols} in {source}")
                    continue

                financial_rows = []
                for _, row in df.iterrows():
                    row_date = row['date'].strftime('%Y-%m-%d')
                    if last_data_main and row_date <= last_data_main.strftime('%Y-%m-%d'):
                        continue  # Skip rows that have already been fetched
                    financial_rows.append({
                        'series_id': source,
                        'date': row_date,
                        'open': float(row['open']) if pd.notna(row['open']) else None,
                        'high': float(row['high']) if pd.notna(row['high']) else None,
                        'low': float(row['low']) if pd.notna(row['low']) else None,
                        'close': float(row['close']) if pd.notna(row['close']) else None,
                        'volume': int(row['volume']) if pd.notna(row['volume']) else None,
                    })
                if financial_rows:
                    load_csv_in_chunks_to_bq(self.FINANCIAL_TABLE_ID, financial_rows, financial_schema)
                    # update the corresponding ds_instance.last_data_date here after successful load.
                    new_latest_date = max(row['date'] for row in financial_rows)
                    ds_instance.metadata['last_fetched_date'] = new_latest_date
                    ds_instance.last_data_date = new_latest_date  # update last_data_date here
                    ds_instance.save()
                    logger.info(f"Economic series {ds_instance.series_id} updated: last_fetched_date and last_data_date are now {new_latest_date}")
                else:
                    print(f"No new financial data for {source}")
                continue

            # Process economic CSV:
            # For economic files, we want one DataSeries instance for the file.
            # Every non-date column is a subcomponent. We use the main_series as series_id.
            economic_rows = []
            for _, row in df.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d')
                for col in df.columns:
                    if col == 'date':
                        continue
                    val = row[col]
                    if pd.notna(val):
                        # Use main_series as the series_id; store col as the component.
                        if last_data_main and date_str <= last_data_main.strftime('%Y-%m-%d'):
                            continue  # Skip rows already fetched
                        economic_rows.append({
                            'series_id': main_series,
                            'date': date_str,
                            'component': col,
                            'value': float(val),
                        })
            if economic_rows:
                load_csv_to_bq(self.ECONOMIC_TABLE_ID, economic_rows, economic_schema)
                new_latest_date = max(datetime.strptime(r['date'], '%Y-%m-%d') for r in economic_rows)
                ds_instance.metadata['last_fetched_date'] = new_latest_date.strftime('%Y-%m-%d')
                ds_instance.last_data_date = new_latest_date
                ds_instance.save()
                logger.info(f"Economic series {ds_instance.series_id} updated: last_data_date is now {new_latest_date}")
            else:
                print(f"No new economic data for {source}")

    def prepare_data(self, fred_series_ids):
        """
        Scrape S&P 500 tickers from Wikipedia => create data list for 'financial'.
        Then create data list for 'economic' from the fred_series_ids dictionary.
        """
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

        # 1) Fetch SP500 (financial) data in small batches
        # for i in range(0, len(data_sp500), BATCH_SIZE):
        #     batch = data_sp500[i : i + BATCH_SIZE]
        #     try:
        #         # This runs your Prefect flow in the main process, sequentially
        #         fetch_and_store_flow(batch)  
        #         self.stdout.write(self.style.SUCCESS(
        #             f"Processed batch from index {i} to {i + BATCH_SIZE}"
        #         ))
        #     except Exception as e:
        #         self.stderr.write(self.style.ERROR(f"Error triggering data fetching tasks: {e}"))
        #         break
        
        # 2) If the economic table is empty, do a bulk load from older data
        if self.table_is_empty(self.ECONOMIC_TABLE_ID):
            self.stdout.write("Loading economic data to BigQuery via csv...")

            historical_rows = []

            # Save Metadata for FRED series
            for item in data_fred:
                series_id = item['series_id']
                data_type = item['data_type']
                data_origin = item['data_origin']

                # 2a) Save metadata => create or update DataSeries
                try:
                    fred_fetcher.save_metadata(series_id, data_type, data_origin)
                    self.stdout.write(self.style.SUCCESS(f"Saved metadata for series: {series_id}"))
                except Exception as e:
                    self.stderr.write(self.style.ERROR(f"Failed to save metadata for {series_id}: {e}"))
                    continue  # Skip to the next series if metadata saving fails

                # 2b) Fetch the entire range directly from FRED
                try:
                    observations = self.fetch_fred_series(series_id)
                    self.stdout.write(self.style.SUCCESS(f"Fetched data for series: {series_id}"))
                except Exception as e:
                    self.stderr.write(self.style.ERROR(f"Failed to fetch data for {series_id}: {e}"))
                    continue  # Skip to the next series if data fetching fails

                # 2c) Filter out NaT or NaN
                observations = observations.dropna(subset=['Date', 'value'])  # drops rows with NaT or NaN
                # Also ensure 'value' is numeric; coerce invalid -> NaN -> drop
                observations['value'] = pd.to_numeric(observations['value'], errors='coerce')
                observations = observations.dropna(subset=['value'])
                
                # 2d) Accumulate for CSV load
                for _, row in observations.iterrows():
                    historical_rows.append({
                        "series_id": series_id,
                        "date": row["Date"].strftime('%Y-%m-%d') if isinstance(row["Date"], pd.Timestamp) else row["Date"],
                        "value": row["value"],
                    })

            # 2e) Bulk load the historical data once
            economic_schema = [
                bigquery.SchemaField("series_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("component", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("value", "FLOAT64", mode="NULLABLE"),
            ]
            try:
                load_csv_to_bq(self.ECONOMIC_TABLE_ID, historical_rows, economic_schema)
                self.stdout.write(self.style.SUCCESS(
                    f"Loaded {len(historical_rows)} older FRED rows into {self.ECONOMIC_TABLE_ID}"
                ))
            except Exception as e:
                self.stderr.write(self.style.ERROR(f"Failed to load data into BigQuery: {e}"))
        else:
            # If the table is NOT empty, just do a normal fetch_and_store_flow
            # to pick up any new data. (No huge CSV load needed.)
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
    
        # 3) Process CSV data
        self.process_csv_data(self.csv_file_paths)
        