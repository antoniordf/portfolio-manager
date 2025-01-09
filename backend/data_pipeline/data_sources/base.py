from abc import ABC, abstractmethod
from typing import Any, Dict, List
from django.conf import settings
import requests
from dashboard.models import DataSeries
from datetime import datetime, timedelta, timezone
import logging
import time
from google.cloud import bigquery
import uuid
from io import StringIO
import csv
from data_pipeline.utils.utils import create_temp_dataset_if_not_exists

logger = logging.getLogger(__name__)

# Set up BigQuery client
client = bigquery.Client()

# Configure your BigQuery tables
ECONOMIC_TABLE_ID = settings.ECONOMIC_TABLE_ID
FINANCIAL_TABLE_ID = settings.FINANCIAL_TABLE_ID

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
        Fetch and MERGE series data for a DataSeries instance into BigQuery,
        ensuring no duplicate rows. We create a temporary staging table, load
        rows there, then MERGE them into the final table based on (series_id, date).

        Args:
            data_series_instance (DataSeries): The DataSeries instance.
            initial_fetch (bool): If True, fetch a limited historical range (e.g., last 5 years).
        """    
        data_type = data_series_instance.data_type
        series_id = data_series_instance.series_id
        logger.info(f"Preparing to save data (MERGE) for {series_id}")

        # Decide date range
        if initial_fetch:
            start_date_dt = datetime.now(timezone.utc) - timedelta(days=5 * 365)
            logger.info(f"Initial fetch for {series_id} from {start_date_dt.date()}")
        else:
            last_fetched_date = data_series_instance.metadata.get('last_fetched_date', None)
            if last_fetched_date:
                start_dt = datetime.strptime(last_fetched_date, '%Y-%m-%d').replace(tzinfo=timezone.utc) + timedelta(days=1)
                start_date_dt = start_dt
                logger.info(f"Fetching new data for {series_id} from {start_date_dt.date()}")
            else:
                start_date_dt = datetime.now(timezone.utc) - timedelta(days=5 * 365)
                logger.info(f"No last_fetched_date. Starting from {start_date_dt.date()}")

        end_date_dt = datetime.now(timezone.utc)

        if start_date_dt > end_date_dt:
            logger.info(f"{series_id} is already up to date. Skipping fetch.")
            return

        start_date_str = start_date_dt.strftime('%Y-%m-%d')
        end_date_str = end_date_dt.strftime('%Y-%m-%d')

        # 1) Fetch new data from external API
        try:
            observations = self.fetch_series_data(series_id, start_date_str, end_date_str)
            logger.info(f"Fetched {len(observations)} observations for {series_id}")
        except Exception as e:
            logger.error(f"Failed to fetch data for {series_id}: {e}")
            raise

        if not observations:
            logger.info(f"No new observations for {series_id}, skipping merge.")
            return

        # 2) Build rows for staging
        if data_type == 'financial':
            prod_table_id = FINANCIAL_TABLE_ID
            schema = [
                bigquery.SchemaField("series_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("open", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("high", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("low", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("close", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("volume", "INT64", mode="NULLABLE"),
            ]
            staging_rows = []
            for obs in observations:
                if not initial_fetch:
                    lf = data_series_instance.metadata.get('last_fetched_date')
                    if lf and obs['date'] <= lf:
                        continue
                staging_rows.append({
                    "series_id": series_id,
                    "date": obs['date'],
                    "open": float(obs.get('open', 0)),
                    "high": float(obs.get('high', 0)),
                    "low": float(obs.get('low', 0)),
                    "close": float(obs.get('close', 0)),
                    "volume": int(obs.get('volume', 0)),
                })

        else:  # 'economic'
            prod_table_id = ECONOMIC_TABLE_ID
            schema = [
                bigquery.SchemaField("series_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("value", "FLOAT", mode="NULLABLE"),
            ]
            staging_rows = []
            for obs in observations:
                if not initial_fetch:
                    lf = data_series_instance.metadata.get('last_fetched_date')
                    if lf and obs['date'] <= lf:
                        continue
                staging_rows.append({
                    "series_id": series_id,
                    "date": obs['date'],
                    "value": float(obs.get('value', 0)),
                })

        if not staging_rows:
            logger.info(f"No new rows left to merge for {series_id}. Possibly duplicates or older data.")
            return

        client = bigquery.Client()
        # 1) Ensure staging dataset is present
        staging_dataset_id = create_temp_dataset_if_not_exists()
        safe_series_id = data_series_instance.series_id.replace('.', '_') # BigQuery table names can't have dots and the series_id might have them
        staging_table_id = f"{staging_dataset_id}.{safe_series_id}_staging_{uuid.uuid4().hex[:8]}"

        # Create the staging table
        job_config = bigquery.LoadJobConfig(
            schema=schema, 
            write_disposition="WRITE_TRUNCATE",
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV
        )
        client.create_table(staging_table_id, exists_ok=True)
        logger.info(f"Created staging table: {staging_table_id}")

        # Write rows to CSV in-memory
        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=[f.name for f in schema])
        writer.writeheader()
        for row in staging_rows:
            writer.writerow(row)
        csv_buffer.seek(0)

        load_job = client.load_table_from_file(
            csv_buffer,
            staging_table_id,
            job_config=job_config
        )
        load_job.result()  # wait
        logger.info(f"Loaded {len(staging_rows)} rows into staging table {staging_table_id}")

        # MERGE logic: skip updating existing rows. We only insert rows that don’t exist.
        if data_type == 'financial':
            merge_sql = f"""
            MERGE `{prod_table_id}` T
            USING `{staging_table_id}` S
            ON T.series_id = S.series_id AND T.date = S.date
            WHEN NOT MATCHED THEN
            INSERT (series_id, date, open, high, low, close, volume)
            VALUES (S.series_id, S.date, S.open, S.high, S.low, S.close, S.volume)
            """
        else:
            merge_sql = f"""
            MERGE `{prod_table_id}` T
            USING `{staging_table_id}` S
            ON T.series_id = S.series_id AND T.date = S.date
            WHEN NOT MATCHED THEN
            INSERT (series_id, date, value)
            VALUES (S.series_id, S.date, S.value)
            """

        merge_job = client.query(merge_sql)
        merge_job.result()
        logger.info(f"MERGE completed from staging table to {prod_table_id} (INSERT only).")

        # Clean up staging table
        client.delete_table(staging_table_id, not_found_ok=True)
        logger.info(f"Deleted staging table {staging_table_id}")

        # Update last_fetched_date
        new_latest_date = max(row['date'] for row in staging_rows)
        data_series_instance.metadata['last_fetched_date'] = new_latest_date
        data_series_instance.save()

        # Use timezone-aware now
        data_series_instance.last_updated = datetime.now(timezone.utc)
        data_series_instance.save()

        logger.info(f"Finished MERGE for {series_id}. last_fetched_date is now {new_latest_date}")