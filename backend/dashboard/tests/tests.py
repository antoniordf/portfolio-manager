from django.test import TestCase, Client
from django.urls import reverse
from rest_framework.test import APIClient
from datetime import datetime
from rest_framework import status
from dashboard.models import RealGDP, TreasuryYield, NominalInflation, EconomicDataPoint
from data_pipeline.utils import fetch_and_save_metadata, fetch_and_save_series
from django.conf import settings
import pprint
import json

api_key = settings.FRED_API_KEY

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
    276.434, 273.887, 272.789, 271.994, 270.664,
    268.452, 266.752, 264.910, 263.583, 262.518,
    262.005, 260.895, 260.249, 259.951, 259.366,
    258.408, 257.004, 255.848, 256.126, 258.150,
    259.246, 258.906, 258.630, 257.879, 257.155,
    256.430, 256.036, 255.802, 255.213, 255.296,
    255.233, 254.277, 253.319, 252.561, 252.767,
    252.594, 252.772, 252.182, 251.663, 251.214,
    251.018, 250.792, 250.227, 249.577, 249.529,
    248.859, 247.805, 247.284, 246.626, 246.435,
    245.183, 244.243, 244.163, 244.004, 244.193,
    243.892, 244.006, 243.618
]

class FetchDataAPITestCase(TestCase):

    def test_fetch_and_save_gdp(self):
        # Ensure there are no existing records
        self.assertEqual(RealGDP.objects.count(), 0)
        
        try:
            # Call the function to retrieve metadata
            data_series_instance = fetch_and_save_metadata(api_key, "GDPC1", RealGDP, "fred", data_type='economic')

            # If no instance is returned, fail the test
            self.assertIsNotNone(data_series_instance, "Failed to fetch and save metadata.")

            # Call the function to download and save the time series
            fetch_and_save_series(api_key, data_series_instance, "fred")

            # Check that data was saved to the database
            self.assertGreater(RealGDP.objects.count(), 0)

            # Optionally, check specific records or data values
            latest_record = data_series_instance.economic_data_points.latest('date')
            print(f"Latest GDP Data: {latest_record.date} - {latest_record.value}")

        except Exception as e:
            self.fail(f"Test failed due to an exception: {e}")

    def test_fetch_and_save_treasury_yields(self):
        # Ensure there are no existing records
        self.assertEqual(TreasuryYield.objects.count(), 0)

        treasury_yields = ["DGS1MO", "DGS3MO", "DGS6MO", "DGS1", "DGS2", "DGS3", "DGS5", "DGS7", "DGS10", "DGS20", "DGS30"]
        
        for asset in treasury_yields:
            with self.subTest(asset=asset):
                try:
                    # Call the function to retrieve metadata
                    data_series_instance = fetch_and_save_metadata(api_key, asset, TreasuryYield, "fred", data_type='economic')

                    # If no instance is returned, fail the test
                    self.assertIsNotNone(data_series_instance, f"Failed to fetch and save metadata for {asset}.")

                    # Call the function to download and save the time series
                    fetch_and_save_series(api_key, data_series_instance, "fred")

                except Exception as e:
                    self.fail(f"Test failed for {asset} due to an exception: {e}")

        # After all yields are processed, check that records have been created
        self.assertEqual(TreasuryYield.objects.count(), len(treasury_yields), "Not all treasury yields were saved.")

        # Optionally, check specific records or data values
        for asset in treasury_yields:
            series_instance = TreasuryYield.objects.get(series_id=asset)
            latest_record = series_instance.economic_data_points.latest('date')
            print(f"Latest Treasury Yield Data for {asset}: {latest_record.date} - {latest_record.value}")

class CalculateReturnsTest(TestCase):
        
    def setUp(self):
        # Create RealGDP instance
        self.real_gdp_series = RealGDP.objects.create(
            series_id='GDPC1',
            name='Real Gross Domestic Product (Test)',
            observation_start=datetime(2018, 1, 1).date(),
            observation_end=datetime(2024, 4, 1).date(),
            frequency='Quarterly',
            units='Billions of Dollars',
            seasonal_adjustment='Seasonally Adjusted',
            data_type='economic'
        )

        # Create NominalInflation instance
        self.inflation_series = NominalInflation.objects.create(
            series_id='CPIAUCSL',
            name='Consumer Price Index for All Urban Consumers (Test)',
            observation_start=datetime(2018, 1, 1).date(),
            observation_end=datetime(2024, 9, 1).date(),
            frequency='Monthly',
            units='Index 1982-1984=100',
            seasonal_adjustment='Seasonally Adjusted',
            data_type='economic'
        )

        # Populate RealGDP data points
        for date_str, value in zip(gdp_dates, gdp_values):
            EconomicDataPoint.objects.create(
                series=self.real_gdp_series,
                date=datetime.strptime(date_str, '%Y-%m-%d').date(),
                value=value
            )

        # Populate NominalInflation data points
        for date_str, value in zip(inflation_dates, inflation_values):
            EconomicDataPoint.objects.create(
                series=self.inflation_series,
                date=datetime.strptime(date_str, '%Y-%m-%d').date(),
                value=value
            )

    def test_calculate_returns(self):
        # Call calculate_returns and capture the returned DataFrames
        gdp_returns_df = EconomicDataPoint.calculate_returns(
            self.real_gdp_series.economic_data_points.all(),
            data_frequency=self.real_gdp_series.frequency
        )
        inflation_returns_df = EconomicDataPoint.calculate_returns(
            self.inflation_series.economic_data_points.all(),
            data_frequency=self.inflation_series.frequency
        )

        # Print the DataFrames for debugging
        print("\nGDP Returns DataFrame:")
        print(gdp_returns_df)

        print("\nInflation Returns DataFrame:")
        print(inflation_returns_df)

        # Assertions to verify the return calculations are correct
        # Example: Check that the number of calculated returns matches expected
        expected_gdp_returns = len(gdp_dates) - 12  # Quarterly data with 12 periods for YoY
        expected_inflation_returns = len(inflation_dates) - 12  # Monthly data with 12 periods for YoY

        self.assertEqual(len(gdp_returns_df), expected_gdp_returns, "Incorrect number of GDP returns calculated.")
        self.assertEqual(len(inflation_returns_df), expected_inflation_returns, "Incorrect number of Inflation returns calculated.")    

class QuadrantDataViewTest(TestCase):
    def setUp(self):
        # Initialize APIClient
        self.client = APIClient()

        # Create RealGDP instance
        self.real_gdp_series = RealGDP.objects.create(
            series_id='GDPC1',
            name='Real Gross Domestic Product (Test)',
            observation_start=datetime(2018, 1, 1).date(),
            observation_end=datetime(2024, 4, 1).date(),
            frequency='Quarterly',
            units='Billions of Dollars',
            seasonal_adjustment='Seasonally Adjusted',
            data_type='economic'
        )

        # Create NominalInflation instance
        self.inflation_series = NominalInflation.objects.create(
            series_id='CPIAUCSL',
            name='Consumer Price Index for All Urban Consumers (Test)',
            observation_start=datetime(2018, 1, 1).date(),
            observation_end=datetime(2024, 9, 1).date(),
            frequency='Monthly',
            units='Index 1982-1984=100',
            seasonal_adjustment='Seasonally Adjusted',
            data_type='economic'
        )

        # Populate RealGDP data points
        for date_str, value in zip(gdp_dates, gdp_values):
            EconomicDataPoint.objects.create(
                series=self.real_gdp_series,
                date=datetime.strptime(date_str, '%Y-%m-%d').date(),
                value=value
            )

        # Populate NominalInflation data points
        for date_str, value in zip(inflation_dates, inflation_values):
            EconomicDataPoint.objects.create(
                series=self.inflation_series,
                date=datetime.strptime(date_str, '%Y-%m-%d').date(),
                value=value
            )

    def test_quadrant_data_view(self):
        # Define the GraphQL query
        graphql_query = """
        query QuadrantData($startDate: Date, $endDate: Date, $dataPoints: Int) {
            quadrantData(startDate: $startDate, endDate: $endDate, dataPoints: $dataPoints) {
                date
                gdpGrowth
                inflationGrowth
            }
        }
        """
         # Define variables for the query
        variables = {
            "startDate": "2020-01-01",
            "endDate": "2024-04-01",
            "dataPoints": 15
        }

        # Send a POST request to the GraphQL endpoint
        response = self.client.post(
            '/graphql/',
            data=json.dumps({
                "query": graphql_query,
                "variables": variables
            }),
            content_type='application/json'
        )

        # Check that the response status code is 200 OK
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Parse the response data
        response_data = response.json()

         # Check for errors in the GraphQL response
        if 'errors' in response_data:
            self.fail(f"GraphQL errors occurred: {response_data['errors']}")

        data = response_data.get('data', {}).get('quadrantData', [])

        # Print the output for debugging
        pprint.pprint(data, indent=2)

        # Perform assertions to validate the data
        # For example, check that data is not empty
        self.assertTrue(len(data) > 0, "The data should not be empty.")

        # Create dictionaries mapping dates to GDP and Inflation values
        gdp_dict = {datetime.strptime(d, '%Y-%m-%d').date(): v for d, v in zip(gdp_dates, gdp_values)}
        inflation_dict = {datetime.strptime(d, '%Y-%m-%d').date(): v for d, v in zip(inflation_dates, inflation_values)}

        # Sort the dates
        sorted_gdp_dates = sorted(gdp_dict.keys())
        sorted_inflation_dates = sorted(inflation_dict.keys())

        # Calculate YoY changes for GDP
        gdp_yoy = {}
        for date in sorted_gdp_dates:
            prev_year_date = date.replace(year=date.year - 1)
            if prev_year_date in gdp_dict:
                gdp_yoy[date] = (gdp_dict[date] / gdp_dict[prev_year_date]) - 1

        # Calculate rate_of_change for GDP
        gdp_roc = {}
        sorted_gdp_yoy_dates = sorted(gdp_yoy.keys())
        for i in range(1, len(sorted_gdp_yoy_dates)):
            current_date = sorted_gdp_yoy_dates[i]
            prev_date = sorted_gdp_yoy_dates[i - 1]
            gdp_roc[current_date] = gdp_yoy[current_date] - gdp_yoy[prev_date]

        # Calculate YoY changes for Inflation
        inflation_yoy = {}
        for date in sorted_inflation_dates:
            prev_year_date = date.replace(year=date.year - 1)
            if prev_year_date in inflation_dict:
                inflation_yoy[date] = (inflation_dict[date] / inflation_dict[prev_year_date]) - 1

        # Calculate rate_of_change for Inflation
        inflation_roc = {}
        sorted_inflation_yoy_dates = sorted(inflation_yoy.keys())
        for i in range(1, len(sorted_inflation_yoy_dates)):
            current_date = sorted_inflation_yoy_dates[i]
            prev_date = sorted_inflation_yoy_dates[i - 3]
            inflation_roc[current_date] = inflation_yoy[current_date] - inflation_yoy[prev_date]

        # Now, iterate through the response data and compare
        for dp in data:
            date = datetime.strptime(dp['date'], '%Y-%m-%d').date()
            # Calculate expected gdp_growth
            expected_gdp_growth = gdp_roc.get(date, None)
            expected_inflation_growth = inflation_roc.get(date, None)

            # Check if manual calculations exist
            self.assertIsNotNone(expected_gdp_growth, f"No manual GDP growth calculation for date {date}")
            self.assertIsNotNone(expected_inflation_growth, f"No manual Inflation growth calculation for date {date}")

            # Compare with response data, allowing for some floating point tolerance
            self.assertAlmostEqual(dp['gdpGrowth'], expected_gdp_growth * 100, places=4, msg=f"GDP growth mismatch on {date}")
            print(dp['gdpGrowth'], expected_gdp_growth * 100, date)
            self.assertAlmostEqual(dp['inflationGrowth'], expected_inflation_growth * 100, places=4, msg=f"Inflation growth mismatch on {date}")
            print(dp['inflationGrowth'], expected_inflation_growth * 100, date)
            

