import requests
from celery import shared_task
from django.conf import settings
from data_fetch.models import DataSeries, DataPoint
from datetime import datetime, timedelta
from decimal import Decimal
import time

API_KEY = settings.POLYGON_API_KEY

@shared_task
def fetch_and_store_data(symbols):
    for symbol in symbols:
        # Get or create DataSeries instance
        series, created = DataSeries.objects.get_or_create(
            symbol=symbol,
            defaults={'name': symbol}  # provide a default name or fetch it from an API
        )

        # Determine the start date for fetching data
        if created:
            # If the series is new, fetch from a default start date
            start_date = '2000-01-01'
        else:
            # Get the latest date we have data for this series
            latest_data_point = DataPoint.objects.filter(series=series).order_by('-date').first()
            if latest_data_point:
                # Start from the day after the latest date
                start_date = (latest_data_point.date + timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                start_date = '2000-01-01'

        end_date = datetime.today().strftime('%Y-%m-%d')

        data = fetch_data_from_polygon(symbol, start_date, end_date)
        if data:
            store_data_points(series, data)
        else:
            print(f"No data fetched for {symbol}")

        # Sleep to respect API rate limits (adjust as necessary)
        time.sleep(15)

def fetch_data_from_polygon(symbol, start_date, end_date):
    try:
        url = f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}'
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000,
            'apiKey': API_KEY
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        json_data = response.json()
        if json_data.get('results'):
            return json_data['results']
        else:
            print(f"No data returned for {symbol}")
            return None
    except requests.RequestException as e:
        print(f"Network error fetching data for {symbol}: {e}")
        return None

def store_data_points(series, data):
    for item in data:
        timestamp = item['t'] / 1000  # Convert from milliseconds to seconds
        date = datetime.fromtimestamp(timestamp).date()
        value = Decimal(str(item['c']))  # Closing price

        # Update or create DataPoint
        DataPoint.objects.update_or_create(
            series=series,
            date=date,
            defaults={'value': value}
        )