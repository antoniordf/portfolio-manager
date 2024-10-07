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
            {
                'name': '2-Year Treasury Yield',
                'series_id': 'GS2',
                'model': TreasuryYield,
                'data_origin': 'fred',
            },
            {
                'name': '5-Year Treasury Yield',
                'series_id': 'GS5',
                'model': TreasuryYield,
                'data_origin': 'fred',
            },
            {
                'name': '7-Year Treasury Yield',
                'series_id': 'GS7',
                'model': TreasuryYield,
                'data_origin': 'fred',
            },
            {
                'name': '15-Year Treasury Yield',
                'series_id': 'GS15',
                'model': TreasuryYield,
                'data_origin': 'fred',
            },
            {
                'name': '30-Year Treasury Yield',
                'series_id': 'GS30',
                'model': TreasuryYield,
                'data_origin': 'fred',
            },
            {
                'name': 'VIX (CBOE Volatility Index)',
                'series_id': 'VIXCLS',
                'model': VIX,
                'data_origin': 'fred',
            },
            # Add other indicators not available in FRED as placeholders
            # {
            #     'name': 'Real Interest Rate',
            #     'series_id': None,  # No FRED series ID
            #     'model': RealInterestRate,
            #     'data_origin': 'custom',
            # },
        ]

        # Check if the database is empty
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public';")
            result = cursor.fetchone()
            is_db_empty = result[0] == 0

        if is_db_empty:
            self.stdout.write("Database is empty. Downloading all data.")
        else:
            self.stdout.write("Database is not empty. Checking for updates.")

        for indicator in indicators:
            name = indicator['name']
            series_id = indicator['series_id']
            model_class = indicator['model']
            data_origin = indicator['data_origin']

            self.stdout.write(f"Processing {name}...")

            try:
                if data_origin == 'fred':
                    if not model_class.objects.filter(series_id=series_id).exists():
                        data_series_instance = fetch_and_save_metadata(api_key, series_id, model_class, data_origin)
                        self.stdout.write(self.style.SUCCESS(f"{name} metadata downloaded and saved."))
                    else:
                        data_series_instance = model_class.objects.get(series_id=series_id)
                        self.stdout.write(f"{name} metadata already exists.")

                    fetch_and_save_series(api_key, data_series_instance, data_origin)
                    self.stdout.write(self.style.SUCCESS(f"{name} data points updated."))

                elif data_origin == 'custom':
                    self.stdout.write(f"{name} is not available on FRED. Please implement custom data fetching.")
                    # Implement custom data fetching logic here
                    # Example:
                    # data_series_instance = fetch_and_save_custom_metadata(...)
                    # fetch_and_save_custom_series(...)
                else:
                    self.stdout.write(f"Data origin '{data_origin}' not recognized for {name}.")

            except Exception as e:
                self.stderr.write(self.style.ERROR(f"Error processing {name}: {e}"))
                # Continue with the next indicator
                continue

        self.stdout.write(self.style.SUCCESS("Data seeding completed."))