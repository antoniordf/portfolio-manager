from django.test import TestCase

from .models import TreasuryYield
from .models import ConcreteDataSeries
from .models.real_gdp import RealGDP
from .utils import fetch_and_save_metadata, fetch_and_save_series
from django.conf import settings

api_key = settings.FRED_API_KEY

class FetchDataAPITestCase(TestCase):

    #Test to download and save GDP data from FRED database
    def test_fetch_and_save_gdp(self):

        # Ensure there are no existing records
        self.assertEqual(RealGDP.objects.count(), 0)
        
        try:
            # Call the function to retrieve metadata
            data_series_instance = fetch_and_save_metadata(api_key, "GDPC1", RealGDP, "fred")

            # If no instance is returned, fail the test
            self.assertIsNotNone(data_series_instance, "Failed to fetch and save metadata.")

            # Call the function to download and save the time series
            fetch_and_save_series(api_key, data_series_instance, "fred")

            # Check that data was saved to the database
            self.assertGreater(RealGDP.objects.count(), 0)

            # Optionally, check specific records or data values
            latest_record = data_series_instance.data_points.latest('date')
            print(f"Latest GDP Data: {latest_record.date} - {latest_record.value}")

        except Exception as e:
            self.fail(f"Test failed due to an exception: {e}")

    # Test to download and save treasury yield data from FRED database
    def test_fetch_and_save_treasury_yields(self):

        # Ensure there are no existing records
        self.assertEqual(TreasuryYield.objects.count(), 0)

        treasury_yields = ["DGS1MO", "DGS3MO", "DGS6MO", "DGS1", "DGS2", "DGS3", "DGS5", "DGS7", "DGS10", "DGS20", "DGS30"]
        
        for asset in treasury_yields:
            try:
                # Call the function to retrieve metadata
                data_series_instance = fetch_and_save_metadata(api_key, asset, TreasuryYield, "fred")

                # If no instance is returned, fail the test
                self.assertIsNotNone(data_series_instance, "Failed to fetch and save metadata.")

                # Call the function to download and save the time series
                fetch_and_save_series(api_key, data_series_instance, "fred")

                # Check that data was saved to the database
                self.assertGreater(TreasuryYield.objects.count(), 0)

                # Optionally, check specific records or data values
                latest_record = data_series_instance.data_points.latest('date')
                print(f"Latest Treasury Yield Data: {latest_record.date} - {latest_record.value}")

            except Exception as e:
                self.fail(f"Test failed due to an exception: {e}")

class CalculateReturns(TestCase):
        
        def test_calculate_returns(self):
            # Ensure there are no existing records
            self.assertEqual(ConcreteDataSeries.objects.count(), 0)
            
            # Create a new ConcreteDataSeries instance
            data_series_instance = ConcreteDataSeries.objects.create(name="Test Series")
    
            # Add some data points
            data_points = [
                (1, 100.0),
                (2, 105.0),
                (3, 110.0),
                (4, 95.0),
                (5, 100.0)
            ]
            
            for date, value in data_points:
                data_series_instance.data_points.create(date=date, value=value)
    
            # Calculate returns
            data_series_instance.calculate_returns()
    
            # Check that the returns were calculated correctly
            returns = data_series_instance.returns.all().order_by('date')
            expected_returns = [0.05, 0.04761905, -0.13636364, 0.05263158]
    
            for i, ret in enumerate(returns):
                self.assertAlmostEqual(ret.value, expected_returns[i], places=6)
    
            print("Calculated Returns:")
            for ret in returns:
                print(f"{ret.date} - {ret.value}")
    
            print("Expected Returns:")
            for i, ret in enumerate(expected_returns):
                print(f"{data_points[i+1][0]} - {ret}")
    
            # Optionally, check specific records or data values
            latest_return = data_series_instance.returns.latest('date')
            print(f"Latest Return: {latest_return.date} - {latest_return.value}")
    
            # Check that the number of returns matches the expected number
            self.assertEqual(returns.count(), len(expected_returns))