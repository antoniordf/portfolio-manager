from django.core.management.base import BaseCommand
from base.utils import fetch_and_save_metadata, fetch_and_save_series
from base.models.real_gdp import RealGDP
from django.conf import settings

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        api_key = settings.FRED_API_KEY
        data_series_instance = fetch_and_save_metadata(api_key, "GDPC1", RealGDP, "fred")
        self.stdout.write("Metadata downloaded and saved")
        fetch_and_save_series(api_key, data_series_instance, "fred")
        self.stdout.write("Series data downloaded and saved")