import requests
from dashboard.models import DataSeries
from django.contrib.contenttypes.models import ContentType
from dashboard.models import EconomicDataPoint, FinancialDataPoint
from datetime import datetime, timedelta
from google.cloud import bigquery
import io
import csv

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
    Fetches time series data for a given DataSeries instance from the specified data source and saves it to the database.
    
    Parameters:
    - api_key: Your API key.
    - data_series_instance: An instance of a DataSeries subclass (e.g., RealGDP) where the data will be saved.
    - data_origin: The source of the data (e.g., 'fred', 'quandl', 'yahoofinance').
    
    Returns:
    - None
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
    
def load_historical_economic_data_to_bq(table_id: str, data_rows: list[dict]):
    """
    Loads older (or any) economic data into BigQuery via a load job.

    Expects data_rows in the form:
    [
      {
        "series_id": "GDPC1",
        "date": "1960-01-01",
        "value": 123.45
      },
      {
        "series_id": "CPIAUCSL",
        "date": "1960-01-01",
        "value": 99.99
      },
      ...
    ]

    'table_id' is the full BigQuery table identifier, e.g.:
        "your-project.your_dataset.economic_data_points"
    """

    client = bigquery.Client()

    # 1) Convert data_rows into an in-memory CSV with series_id, date, value
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(["series_id", "date", "value"])  # CSV header

    for row in data_rows:
        writer.writerow([
            row["series_id"],
            row["date"],
            row["value"],
        ])

    csv_buffer.seek(0)  # rewind

    # 2) Configure the load job
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("series_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("value", "FLOAT64", mode="NULLABLE"),
        ],
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",  # or "WRITE_TRUNCATE" to overwrite
    )

    # 3) Run the load job
    load_job = client.load_table_from_file(
        csv_buffer,
        table_id,
        job_config=job_config,
    )
    load_job.result()  # Wait for it to finish

    print(f"Loaded {len(data_rows)} economic rows into '{table_id}'.")