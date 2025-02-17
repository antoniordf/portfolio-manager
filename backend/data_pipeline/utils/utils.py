import logging
from django.conf import settings
import requests
from datetime import datetime
from google.cloud import bigquery
import io
import csv
import pandas as pd

client = bigquery.Client()
ECONOMIC_TABLE_ID = settings.ECONOMIC_TABLE_ID
FINANCIAL_TABLE_ID = settings.FINANCIAL_TABLE_ID

logger = logging.getLogger(__name__)

def fetch_and_save_metadata(api_key, series_id, DataSeriesClass, data_origin, data_type):
    """
    Fetches metadata for a given series_id from the specified data source and saves it as an instance of DataSeriesClass.
    
    Parameters:
    - api_key: Your API key.
    - series_id: The ID of the series to fetch.
    - DataSeriesClass: The Django model class to use for saving the data (e.g., RealGDP).
    - data_origin: The source of the data (e.g., 'fred', 'quandl', 'yahoofinance').
    
    Returns:
    - An instance of the DataSeriesClass with the metadata saved.
    """
    url = construct_metadata_url(api_key, series_id, data_origin)
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        metadata = parse_metadata(response.json(), data_origin)
        
         # Use update_or_create to handle existing series_ids
        data_series_instance, created = DataSeriesClass.objects.update_or_create(
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
                'metadata': metadata,
            }
        )
    
        if created:
            print(f"Created new DataSeries: {metadata['id']}")
        else:
            print(f"Updated existing DataSeries: {metadata['id']}")
        
        return data_series_instance
    except requests.exceptions.RequestException as e:
        # Handle request errors
        print(f"Error fetching metadata: {e}")
        raise

def fetch_and_save_series(api_key, data_series_instance, data_origin):
    """
    Fetch time series data for data_series_instance from the data_origin
    and writes it into BigQuery. No reference to local Django model points.
    Checks BigQuery to avoid duplicates if desired.
    """
    # Check if there is existing data in the database
    if data_series_instance.data_type == 'economic':
        data_point_model = EconomicDataPoint
    elif data_series_instance.data_type == 'financial':
        data_point_model = FinancialDataPoint
    else:
        raise ValueError(f"Unknown data_type: {data_series_instance.data_type}")

    # Check if there is existing data in the database
    if data_point_model.objects.filter(series=data_series_instance).exists():
        last_date = data_point_model.objects.filter(series=data_series_instance).latest('date').date
    else:
        last_date = None

    url = construct_series_url(api_key, data_series_instance.series_id, data_origin)

    try:
        response = requests.get(url)
        response.raise_for_status()
        observations = parse_series_data(response.json(), data_origin)

        for observation in observations:
            date_str = observation['date']
            date = datetime.strptime(date_str, '%Y-%m-%d').date()

            if last_date and date <= last_date:
                continue

            if data_series_instance.data_type == 'economic':
                value = observation['value']
                try:
                    value = float(value)
                except ValueError:
                    continue  # Skip non-numeric values

                data_point_model.objects.create(
                    series=data_series_instance,
                    date=date,
                    value=value
                )

            elif data_series_instance.data_type == 'financial':
                # Assuming observation contains 'open', 'high', 'low', 'close', 'volume'
                data_point_model.objects.create(
                    series=data_series_instance,
                    date=date,
                    open=float(observation.get('open', 0)),
                    high=float(observation.get('high', 0)),
                    low=float(observation.get('low', 0)),
                    close=float(observation.get('close', 0)),
                    volume=int(observation.get('volume', 0))
                )

    except requests.exceptions.RequestException as e:
        print(f"Error fetching series data: {e}")
        raise

def construct_metadata_url(api_key, series_id, data_origin):
    if data_origin == 'fred':
        return f'https://api.stlouisfed.org/fred/series/search?search_text={series_id}&api_key={api_key}&file_type=json'
    # elif data_origin == 'quandl':
    #     return f'https://www.quandl.com/api/v3/datasets/{series_id}.json?api_key={api_key}'
    # elif data_origin == 'yahoofinance':
    #     return f'https://query1.finance.yahoo.com/v7/finance/options/{series_id}'

def construct_series_url(api_key, series_id, data_origin):
    if data_origin == 'fred':
        return f'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json'
    # elif data_origin == 'quandl':
    #     return f'https://www.quandl.com/api/v3/datasets/{series_id}/data.json?api_key={api_key}'
    # elif data_origin == 'yahoofinance':
    #     return f'https://query1.finance.yahoo.com/v7/finance/options/{series_id}'

def parse_metadata(response_json, data_origin):
    if data_origin == 'fred':
        metadata = response_json['seriess'][0]
        return {
            'id': metadata['id'],
            'title': metadata['title'],
            'observation_start': metadata['observation_start'],
            'observation_end': metadata['observation_end'],
            'frequency': metadata.get('frequency', 'N/A'),
            'units': metadata.get('units', 'N/A'),
            'seasonal_adjustment': metadata.get('seasonal_adjustment', 'N/A'),
            'last_updated': metadata['last_updated'],
            'notes': metadata.get('notes', '')
        }

def parse_series_data(response_json, data_origin):
    if data_origin == 'fred':
        return [{'date': obs['date'], 'value': obs['value']} for obs in response_json['observations']]
    
def load_csv_in_chunks_to_bq(table_id: str, data_rows: list[dict], schema: list[bigquery.SchemaField], chunk_size_days: int = 3000):
    """
    Splits the data_rows into chunks by date (using chunk_size_days as the maximum span per chunk)
    and loads each chunk into BigQuery by calling load_csv_to_bq.

    Args:
        table_id: Fully-qualified BigQuery table id (e.g., "project.dataset.table").
        data_rows: List of dictionaries representing rows.
        schema: List of BigQuery SchemaField objects for the target table.
        chunk_size_days: Maximum number of days per chunk (default is 3000 days).
    """
    if not data_rows:
        return

    # Convert the list of rows into a DataFrame and ensure 'date' is a datetime column.
    df = pd.DataFrame(data_rows)
    df['date'] = pd.to_datetime(df['date'], dayfirst=False)
    df = df.dropna(subset=['date'])
    df.sort_values(by='date', inplace=True)

    # Determine the date range
    min_date = df['date'].min()
    max_date = df['date'].max()

    # Iterate over the full range in increments of chunk_size_days.
    current_start = min_date
    while current_start < max_date:
        current_end = current_start + pd.Timedelta(days=chunk_size_days)
        # Select the chunk of rows that fall within this date interval.
        chunk_df = df[(df['date'] >= current_start) & (df['date'] < current_end)].copy()
        if not chunk_df.empty:
            # Convert the 'date' column to strings in 'YYYY-MM-DD' format
            chunk_df = chunk_df.assign(date=chunk_df['date'].dt.strftime('%Y-%m-%d'))
            # Convert the chunk back to a list of dictionaries.
            chunk_rows = chunk_df.to_dict(orient='records')
            # Call your existing load_csv_to_bq function for this chunk.
            load_csv_to_bq(table_id, chunk_rows, schema)
        current_start = current_end
    
def load_csv_to_bq(table_id: str, data_rows: list[dict], schema: list[bigquery.SchemaField]):
    client = bigquery.Client()

    # Deduplicate rows
    unique_new_rows = []
    seen = set()
    for row in data_rows:
        key = (row.get('series_id'), row.get('date'), row.get('component'))
        if key not in seen:
            seen.add(key)
            unique_new_rows.append(row)

    # Filter out rows with missing or invalid values for required fields
    filtered_rows = []
    for row in unique_new_rows:
        valid = True
        new_row = {}
        for field in schema:
            value = row.get(field.name)
            # For REQUIRED fields, if missing or invalid, mark row as invalid.
            if field.mode == "REQUIRED":
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    valid = False
                    break
            # For numeric fields, replace NaN with None
            if field.field_type in ["FLOAT64", "INTEGER"]:
                if isinstance(value, float) and pd.isna(value):
                    value = None
            new_row[field.name] = value
        if valid:
            filtered_rows.append(new_row)

    # Convert rows to CSV in memory
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=[f.name for f in schema])
    writer.writeheader()
    writer.writerows(filtered_rows)
    csv_buffer.seek(0)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    load_job = client.load_table_from_file(csv_buffer, table_id, job_config=job_config)
    load_job.result()  # Wait for completion
    print(f"Loaded {len(filtered_rows)} rows into {table_id}.")
    
    
def create_temp_dataset_if_not_exists() -> str:
    """
    Ensures that a staging dataset for merges/temporary tables exists in BigQuery.
    Returns the full "project.dataset" string.

    Expects:
      settings.STAGING_DATASET_ID = "myproject.my_staging_dataset"
    """
    staging_dataset_id = settings.STAGING_DATASET_ID  # e.g. "myproject.my_staging_dataset"
    client = bigquery.Client()

    try:
        # Attempt to get the dataset; if it doesn't exist, an exception is raised
        client.get_dataset(staging_dataset_id)
        logger.info(f"Staging dataset {staging_dataset_id} already exists.")
    except Exception:
        # Create dataset
        project, dataset_name = staging_dataset_id.split(".", 1)
        dataset = bigquery.Dataset(staging_dataset_id)
        dataset.location = "US"  # or your preferred location
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Created staging dataset {staging_dataset_id} in project={project}.")

    return staging_dataset_id