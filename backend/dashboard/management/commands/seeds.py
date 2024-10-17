from django.core.management.base import BaseCommand
from dashboard.utils import fetch_and_save_metadata, fetch_and_save_series
from django.conf import settings
from django.db import connection
from dashboard.models.real_gdp import RealGDP
from dashboard.models.corporate_bond_spread import CorporateBondSpread
from dashboard.models.fed_funds_rate import FedFundsRate
from dashboard.models.ISM_manufacturing import ISMManufacturing
from dashboard.models.ISM_services import ISMServices
from dashboard.models.nominal_inflation import NominalInflation
from dashboard.models.sp_500 import Sp500
from dashboard.models.treasury_yield import TreasuryYield
from dashboard.models.vix import VIX

class Command(BaseCommand):
    help = 'Seeds the database with initial data for macroeconomic indicators.'

    def handle(self, *args, **kwargs):
        api_key = settings.FRED_API_KEY

        # List of indicators with their FRED series IDs and corresponding model classes
        indicators = [
            {
                'name': 'Real GDP',
                'series_id': 'GDPC1',
                'model': RealGDP,
                'data_origin': 'fred',
            },
            {
                'name': 'Corporate Bond Spread',
                'series_id': 'BAA10Y',
                'model': CorporateBondSpread,
                'data_origin': 'fred',
            },
            {
                'name': 'Federal Funds Rate',
                'series_id': 'FEDFUNDS',
                'model': FedFundsRate,
                'data_origin': 'fred',
            },
            {
                'name': 'Nominal Inflation (CPI)',
                'series_id': 'CPIAUCSL',
                'model': NominalInflation,
                'data_origin': 'fred',
            },
            {
                'name': 'S&P 500 Index',
                'series_id': 'SP500',
                'model': Sp500,
                'data_origin': 'fred',
            },
            {
                'name': '10-Year Treasury Yield',
                'series_id': 'GS10',
                'model': TreasuryYield,
                'data_origin': 'fred',
            },
            # Add other indicators as needed
        ]

        for indicator in indicators:
            name = indicator['name']
            series_id = indicator['series_id']
            model_class = indicator['model']
            data_origin = indicator['data_origin']

            self.stdout.write(f"Processing {name}...")

            try:
                if data_origin == 'fred':
                    # Create or get the series instance
                    series_instance, created = model_class.objects.get_or_create(
                        series_id=series_id,
                        defaults={
                            'name': name,
                            'observation_start': None,  # These will be updated by fetch_and_save_metadata
                            'observation_end': None,
                            'frequency': '',
                            'units': '',
                            'seasonal_adjustment': '',
                            'notes': '',
                        }
                    )

                    # Fetch and save metadata
                    fetch_and_save_metadata(api_key, series_id, series_instance, data_origin)
                    self.stdout.write(self.style.SUCCESS(f"{name} metadata downloaded and saved."))

                    # Fetch and save data points
                    fetch_and_save_series(api_key, series_instance, data_origin)
                    self.stdout.write(self.style.SUCCESS(f"{name} data points updated."))

                else:
                    self.stdout.write(f"Data origin '{data_origin}' not recognized for {name}.")

            except Exception as e:
                self.stderr.write(self.style.ERROR(f"Error processing {name}: {e}"))
                continue

        self.stdout.write(self.style.SUCCESS("Data seeding completed."))