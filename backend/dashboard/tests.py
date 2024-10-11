from django.test import TestCase, Client
from django.urls import reverse
from rest_framework.test import APIClient
from datetime import datetime
from rest_framework import status
from dashboard.models import RealGDP, TreasuryYield, ConcreteDataSeries, NominalInflation, DataPoint
from .utils import fetch_and_save_metadata, fetch_and_save_series
from django.conf import settings
import pprint

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

class QuadrantDataViewTest(TestCase):
    def setUp(self):
        # Create test instances for RealGDP and NominalInflation
        self.real_gdp_series = RealGDP.objects.create(
            series_id='GDPC1',
            name='Real Gross Domestic Product (Test)',
            observation_start=datetime(2018, 1, 1),
            observation_end=datetime(2024, 4, 1),
            frequency='Quarterly',
            units='Billions of Dollars',
            last_updated=datetime.now()
        )

        self.inflation_series = NominalInflation.objects.create(
            series_id='CPIAUCSL',
            name='Consumer Price Index for All Urban Consumers (Test)',
            observation_start=datetime(2018, 1, 1),
            observation_end=datetime(2024, 9, 1),
            frequency='Monthly',
            units='Index 1982-1984=100',
            last_updated=datetime.now()
        )

        # Create test time series data
        # Create GDP data (Quarterly)
        gdp_dates = [
            "2024-04-01", "2024-01-01", "2023-10-01", "2023-07-01", "2023-04-01",
            "2023-01-01", "2022-10-01", "2022-07-01", "2022-04-01", "2022-01-01",
            "2021-10-01", "2021-07-01", "2021-04-01", "2021-01-01", "2020-10-01",
            "2020-07-01", "2020-04-01", "2020-01-01", "2019-10-01", "2019-07-01",
            "2019-04-01", "2019-01-01", "2018-10-01", "2018-07-01", "2018-04-01",
            "2018-01-01", "2017-10-01", "2017-07-01", "2017-04-01", "2017-01-01"
        ]
        gdp_values = [
            23223.906, 23053.545, 22960.600, 22780.933, 22539.418,
            22403.435, 22249.459, 22066.784, 21919.222, 21903.850,
            21960.388, 21571.421, 21389.005, 21058.379, 20771.691,
            20548.793, 19056.617, 20693.238, 20985.448, 20843.322,
            20602.275, 20431.641, 20304.874, 20276.154, 20150.476,
            20044.077, 19882.352, 19660.766, 19506.949, 19398.343
        ]

        for date_str, value in zip(gdp_dates, gdp_values):
            DataPoint.objects.create(
                series=self.real_gdp_series,
                date=datetime.strptime(date_str, '%Y-%m-%d'),
                value=value
            )

        # Create Inflation data (Monthly)
        inflation_dates = [
            "2024-09-01", "2024-08-01", "2024-07-01", "2024-06-01", "2024-05-01",
            "2024-04-01", "2024-03-01", "2024-02-01", "2024-01-01", "2023-12-01",
            "2023-11-01", "2023-10-01", "2023-09-01", "2023-08-01", "2023-07-01",
            "2023-06-01", "2023-05-01", "2023-04-01", "2023-03-01", "2023-02-01",
            "2023-01-01", "2022-12-01", "2022-11-01", "2022-10-01", "2022-09-01",
            "2022-08-01", "2022-07-01", "2022-06-01", "2022-05-01", "2022-04-01",
            "2022-03-01", "2022-02-01", "2022-01-01", "2021-12-01", "2021-11-01",
            "2021-10-01", "2021-09-01", "2021-08-01", "2021-07-01", "2021-06-01",
            "2021-05-01", "2021-04-01", "2021-03-01", "2021-02-01", "2021-01-01",
            "2020-12-01", "2020-11-01", "2020-10-01", "2020-09-01", "2020-08-01",
            "2020-07-01", "2020-06-01", "2020-05-01", "2020-04-01", "2020-03-01",
            "2020-02-01", "2020-01-01", "2019-12-01", "2019-11-01", "2019-10-01",
            "2019-09-01", "2019-08-01", "2019-07-01", "2019-06-01", "2019-05-01",
            "2019-04-01", "2019-03-01", "2019-02-01", "2019-01-01", "2018-12-01",
            "2018-11-01", "2018-10-01", "2018-09-01", "2018-08-01", "2018-07-01",
            "2018-06-01", "2018-05-01", "2018-04-01", "2018-03-01", "2018-02-01",
            "2018-01-01", "2017-12-01", "2017-11-01", "2017-10-01", "2017-09-01",
            "2017-08-01", "2017-07-01", "2017-06-01", "2017-05-01", "2017-04-01",
            "2017-03-01", "2017-02-01", "2017-01-01"
        ]
        inflation_values = [
            314.686, 314.121, 313.534, 313.049, 313.225,
            313.207, 312.230, 311.054, 309.685, 308.742,
            308.024, 307.531, 307.288, 306.187, 304.628,
            304.003, 303.365, 303.032, 301.744, 301.509,
            300.356, 298.812, 298.648, 297.863, 296.341,
            295.209, 294.977, 294.996, 291.359, 288.764,
            287.553, 284.535, 282.390, 280.808, 278.799,
            276.434, 273.887, 272.789, 271.994, 270.064,
            268.452, 266.752, 264.910, 263.583, 262.518,
            262.005, 260.895, 260.249, 259.951, 259.366,
            258.408, 257.004, 255.848, 256.126, 258.150,
            259.246, 258.906, 258.630, 257.879, 257.155,
            256.430, 256.036, 255.802, 255.213, 255.296,
            255.233, 254.277, 253.319, 252.561, 252.767,
            252.594, 252.772, 252.182, 251.683, 251.214,
            251.018, 250.792, 250.227, 249.577, 249.529,
            248.859, 247.805, 247.284, 246.626, 246.435,
            245.183, 244.433, 244.163, 244.004, 244.193,
            244.103, 243.892, 243.006, 243.618,
        ]

        for date_str, value in zip(inflation_dates, inflation_values):
            DataPoint.objects.create(
                series=self.inflation_series,
                date=datetime.strptime(date_str, '%Y-%m-%d'),
                value=value
            )

    def test_quadrant_data_view(self):
        # Use APIClient to make the request
        client = APIClient()

        # Construct the URL for the QuadrantDataView
        url = reverse('dashboard:quadrant_data')

        # Send a GET request to the view
        response = client.get(url)

        # Check that the response status code is 200 OK
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Parse the response data
        data = response.json()

        # Print the output for debugging
        pprint.pprint(data, indent=2)

        # Perform assertions to validate the data
        # For example, check that data is not empty
        self.assertTrue(len(data) > 0, "The data should not be empty.")

        # You can also check the contents of the data
        for item in data:
            # Ensure required keys are present
            self.assertIn('date', item)
            self.assertIn('gdp_growth', item)
            self.assertIn('inflation_growth', item)

            # check that the values are numbers
            self.assertIsInstance(item['gdp_growth'], float)
            self.assertIsInstance(item['inflation_growth'], float)