import requests
from .models import DataSeries
from django.contrib.contenttypes.models import ContentType
from .models import DataPoint

def fetch_and_save_metadata(api_key, series_id, DataSeriesClass, data_origin):
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
        
        data_series_instance = DataSeriesClass.objects.create(
            series_id=metadata['id'],
            name=metadata['title'],
            observation_start=metadata['observation_start'],
            observation_end=metadata['observation_end'],
            frequency=metadata.get('frequency', 'N/A'),
            units=metadata.get('units', 'N/A'),
            seasonal_adjustment=metadata.get('seasonal_adjustment', 'N/A'),
            last_updated=metadata['last_updated'],
            notes=metadata.get('notes', '')
        )
        
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
    if data_series_instance.data_points.exists():
        last_date = data_series_instance.data_points.latest('date').date
    else:
        last_date = None
    
    url = construct_series_url(api_key, data_series_instance.series_id, data_origin)
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        observations = parse_series_data(response.json(), data_origin)

        content_type = ContentType.objects.get_for_model(data_series_instance)
        
        for observation in observations:
            date = observation['date']
            if last_date and date <= last_date:
                # Skip data points that are already present
                continue
            
            value = observation['value']
            try:
                value = float(value)
            except ValueError:
                # print(f"Skipping non-numeric value '{value}' on {date}")
                continue  # Skip this observation if it can't be converted to float

            DataPoint.objects.create(
                content_type=content_type,
                object_id=data_series_instance.id,
                date=date,
                value=value
            )
    except requests.exceptions.RequestException as e:
        # Handle request errors
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