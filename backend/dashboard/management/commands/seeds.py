from django.core.management.base import BaseCommand
from data_fetch.tasks import fetch_and_store_multiple_series
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Seeds the database with initial data for macroeconomic and financial indicators.'

    def handle(self, *args, **kwargs):
        # Define the list of indicators with their series IDs, data origins, and data types
        data_series_list: List[Dict[str, str]] = [
            # FRED Economic Indicators
            {
                'series_id': 'GDPC1',
                'data_origin': 'fred',
                'data_type': 'economic',
            },
            {
                'series_id': 'FEDFUNDS',
                'data_origin': 'fred',
                'data_type': 'economic',
            },
            {
                'series_id': 'CPIAUCSL',
                'data_origin': 'fred',
                'data_type': 'economic',
            },
            {
                'series_id': 'GS10',
                'data_origin': 'fred',
                'data_type': 'economic',
            },
            {
                'series_id': 'GS2',
                'data_origin': 'fred',
                'data_type': 'economic',
            },
            # Add other FRED indicators as needed

            # Polygon.io Financial Indicators
            {
                'series_id': 'AAPL',
                'data_origin': 'polygon',
                'data_type': 'financial',
            },
            {
                'series_id': 'MSFT',
                'data_origin': 'polygon',
                'data_type': 'financial',
            },
            # Add more Polygon.io series as needed
        ]

        total_series = len(data_series_list)
        self.stdout.write(f"Preparing to seed {total_series} data series...")

        try:
            # Trigger the Celery task to process the series
            fetch_and_store_multiple_series.delay(data_series_list)
            self.stdout.write(self.style.SUCCESS("Data fetching tasks have been successfully triggered."))
        except Exception as e:
            logger.error(f"Error triggering data fetching tasks: {e}")
            self.stderr.write(self.style.ERROR(f"Error triggering data fetching tasks: {e}"))