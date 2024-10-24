from graphene_django.utils.testing import GraphQLTestCase
from django.core.cache import cache
from django.utils import timezone
from dashboard.models import EconomicDataPoint, RealGDP, NominalInflation
from config.schema import schema
import datetime
import json
import pandas as pd

class QuadrantDataChartCacheTests(GraphQLTestCase):
    GRAPHQL_SCHEMA = schema
    GRAPHQL_URL = '/graphql/'

    @classmethod
    def setUpTestData(cls):
        # Create a DataSeries instance
        cls.gdp_series = RealGDP.objects.create(
            name="Gross Domestic Product",
            series_id="GDPC1",
            observation_start=datetime.date(2020, 1, 1),
            observation_end=datetime.date(2024, 10, 21),
            frequency="Quarterly",
            units="Billions of Dollars",
            seasonal_adjustment="Seasonally Adjusted",
            data_origin="fred",
            data_type="economic",
            metadata={
                'name': "Gross Domestic Product",
                'series_id': "GDPC1",
                'observation_start': "2020-01-01",
                'observation_end': "2024-10-21",
                'frequency': "Quarterly",
                'units': "Billions of Dollars",
                'seasonal_adjustment': "Seasonally Adjusted",
                'last_updated': timezone.now().isoformat(),
                'notes': "",
            }
        )

        # Create Inflation DataSeries
        cls.inflation_series = NominalInflation.objects.create(
            name="Consumer Price Index",
            series_id="CPIAUCSL",
            observation_start=datetime.date(2020, 1, 1),
            observation_end=datetime.date(2024, 10, 21),
            frequency="Monthly",
            units="Index 1982-1984=100",
            seasonal_adjustment="Seasonally Adjusted",
            data_origin="fred",
            data_type="economic",
            metadata={
                'name': "Consumer Price Index",
                'series_id': "CPIAUCSL",
                'observation_start': "2020-01-01",
                'observation_end': "2024-10-21",
                'frequency': "Monthly",
                'units': "Index 1982-1984=100",
                'seasonal_adjustment': "Seasonally Adjusted",
                'last_updated': timezone.now().isoformat(),
                'notes': "",
            }
        )

        # Create GDP EconomicDataPoints at quarterly dates starting from 2015-01-01
        gdp_dates = pd.date_range(start='2015-01-01', end='2021-01-01', freq='QS').to_pydatetime().tolist()
        for i, date in enumerate(gdp_dates):
            EconomicDataPoint.objects.create(
                series=cls.gdp_series,
                date=date.date(),
                value=18000 + (i * 500)  # Example values
            )

        # Create Inflation EconomicDataPoints at monthly dates starting from 2018-01-01
        inflation_dates = pd.date_range(start='2018-01-01', end='2021-01-01', freq='MS').to_pydatetime().tolist()
        for i, date in enumerate(inflation_dates):
            EconomicDataPoint.objects.create(
                series=cls.inflation_series,
                date=date.date(),
                value=100 + (i * 0.5)  # Example values
            )

    def setUp(self):
        # Clear the cache before each test
        cache.clear()

    def test_cache_miss_fetches_and_caches_data(self):
        """
        Test that a cache miss results in data being fetched from the database and cached.
        """
        # Define the GraphQL query
        query = '''
            query ($startDate: Date, $endDate: Date, $dataPoints: Int) {
                quadrantData(startDate: $startDate, endDate: $endDate, dataPoints: $dataPoints) {
                    date
                    gdpGrowth
                    inflationGrowth
                }
            }
        '''
        variables = {
            'startDate': '2018-01-01',
            'endDate': '2021-01-01',
            'dataPoints': 10
        }

        # Construct the cache key using the maximum last_updated timestamp
        last_updated_timestamp = int(max(
            self.gdp_series.last_updated.timestamp(),
            self.inflation_series.last_updated.timestamp()
        ))
        cache_key = f"quadrant_data_{variables['startDate']}_{variables['endDate']}_{variables['dataPoints']}_{last_updated_timestamp}"

        # Ensure cache is empty
        self.assertIsNone(cache.get(cache_key), msg="Cache should be empty before the test.")

        # Execute the GraphQL query
        response = self.query(
            query,
            variables=variables
        )

        # Check the response status code
        print(response.content.decode('utf-8'))
        self.assertEqual(response.status_code, 200, msg=f"Expected status code 200, got {response.status_code}")

        # Parse the JSON response
        content = json.loads(response.content.decode('utf-8'))

        # Check for errors in the response
        self.assertNotIn('errors', content, msg=f"GraphQL errors occurred: {content.get('errors')}")

        # Verify data is returned correctly
        self.assertIn('data', content, msg="Response does not contain 'data'")
        self.assertIn('quadrantData', content['data'], msg="Response data does not contain 'quadrantData'")
        self.assertEqual(len(content['data']['quadrantData']), variables['dataPoints'])
        # Additional assertions can be added here based on expected values

        # Verify that data is now cached
        cached_data = cache.get(cache_key)
        self.assertIsNotNone(cached_data, msg="Data was not cached after cache miss.")
        self.assertEqual(len(cached_data), 5)

    def test_cache_hit_returns_cached_data(self):
        """
        Test that a cache hit returns data from the cache without querying the database.
        """
        # Define the variables
        variables = {
            'startDate': '2015-01-01',
            'endDate': '2021-01-01',
            'dataPoints': 10
        }

       # Construct the cache key using the maximum last_updated timestamp
        last_updated_timestamp = int(max(
            self.gdp_series.last_updated.timestamp(),
            self.inflation_series.last_updated.timestamp()
        ))
        cache_key = f"quadrant_data_{variables['startDate']}_{variables['endDate']}_{variables['dataPoints']}_{last_updated_timestamp}"

        # Pre-populate the cache with example data
        cached_data = [
            {
                'date': (datetime.date(2015, 1, 1) + datetime.timedelta(days=90 * i)).isoformat(),
                'gdp_growth': 2.0 + i * 0.1,
                'inflation_growth': 1.5 + i * 0.05
            } for i in range(variables['dataPoints'])
        ]
        cache.set(cache_key, cached_data, timeout=None)

        # Define the GraphQL query
        query = '''
            query ($startDate: Date, $endDate: Date, $dataPoints: Int) {
                quadrantData(startDate: $startDate, endDate: $endDate, dataPoints: $dataPoints) {
                    date
                    gdpGrowth
                    inflationGrowth
                }
            }
        '''

        # Execute the GraphQL query
        response = self.query(
            query,
            variables=variables
        )

        # Check the response status code
        print(response.content.decode('utf-8'))
        self.assertEqual(response.status_code, 200, msg=f"Expected status code 200, got {response.status_code}")

        # Parse the JSON response
        content = json.loads(response.content.decode('utf-8'))

        # Check for errors
        self.assertNotIn('errors', content, msg=f"GraphQL errors occurred: {content.get('errors')}")

        # Verify data is returned from cache
        self.assertIn('data', content, msg="Response does not contain 'data'")
        self.assertIn('quadrantData', content['data'], msg="Response data does not contain 'quadrantData'")
        self.assertEqual(len(content['data']['quadrantData']), variables['dataPoints'])
        for idx, data_point in enumerate(content['data']['quadrantData']):
            expected_data = cached_data[idx]
            self.assertEqual(data_point['date'], expected_data['date'])
            self.assertEqual(data_point['gdpGrowth'], expected_data['gdp_growth'])
            self.assertEqual(data_point['inflationGrowth'], expected_data['inflation_growth'])

    def test_cache_invalidation_on_data_update(self):
        """
        Test that updating data invalidates the old cache and creates a new cache entry.
        """
        # Define the variables
        variables = {
            'startDate': '2015-01-01',
            'endDate': '2021-01-01',
            'dataPoints': 10
        }

        # Initial cache population
        initial_last_updated_timestamp = int(max(
            self.gdp_series.last_updated.timestamp(),
            self.inflation_series.last_updated.timestamp()
        ))
        initial_cache_key = f"quadrant_data_{variables['startDate']}_{variables['endDate']}_{variables['dataPoints']}_{initial_last_updated_timestamp}"
        initial_cached_data = [
            {
                'date': (datetime.date(2015, 1, 1) + datetime.timedelta(days=90 * i)).isoformat(),
                'gdp_growth': 2.0 + i * 0.1,
                'inflation_growth': 1.5 + i * 0.05
            } for i in range(variables['dataPoints'])
        ]
        cache.set(initial_cache_key, initial_cached_data, timeout=None)

        # Update an EconomicDataPoint
        updated_data_point = EconomicDataPoint.objects.get(series=self.gdp_series, date=datetime.date(2019, 4, 1))
        updated_data_point.value = updated_data_point.value + 1000  # Example update
        updated_data_point.save()

        # Save the DataSeries to update last_updated
        self.gdp_series.save()

        # Define the GraphQL query
        query = '''
            query ($startDate: Date, $endDate: Date, $dataPoints: Int) {
                quadrantData(startDate: $startDate, endDate: $endDate, dataPoints: $dataPoints) {
                    date
                    gdpGrowth
                    inflationGrowth
                }
            }
        '''

        # Execute the GraphQL query
        response = self.query(
            query,
            variables=variables
        )

        # Check the response status code
        print(response.content.decode('utf-8'))
        self.assertEqual(response.status_code, 200, msg=f"Expected status code 200, got {response.status_code}")

        # Parse the JSON response
        content = json.loads(response.content.decode('utf-8'))

        # Check for errors
        self.assertNotIn('errors', content, msg=f"GraphQL errors occurred: {content.get('errors')}")

        # Verify that new data is fetched and cached
        new_last_updated_timestamp = int(max(
            self.gdp_series.last_updated.timestamp(),
            self.inflation_series.last_updated.timestamp()
        ))
        new_cache_key = f"quadrant_data_{variables['startDate']}_{variables['endDate']}_{variables['dataPoints']}_{new_last_updated_timestamp}"
        new_cached_data = cache.get(new_cache_key)
        self.assertIsNotNone(new_cached_data, msg="New data was not cached after data update.")
        self.assertEqual(len(new_cached_data), variables['dataPoints'])

        # Verify that the updated value is reflected
        updated_gdp_growth = new_cached_data[1]['gdp_growth']
        
        # Calculate the expected gdpGrowth based on your application logic
        # For example, if gdpGrowth is the percentage change from the previous quarter
        previous_value = EconomicDataPoint.objects.get(series=self.gdp_series, date=datetime.date(2020, 1, 1)).value
        expected_gdp_growth = ((updated_data_point.value - previous_value) / previous_value) * 100
        self.assertAlmostEqual(updated_gdp_growth, expected_gdp_growth, places=2)

        # Verify that the old cache still exists
        old_cached_data = cache.get(initial_cache_key)
        self.assertIsNotNone(old_cached_data, msg="Old cache key was unexpectedly deleted.")
        self.assertEqual(old_cached_data[0]['gdp_growth'], initial_cached_data[0]['gdp_growth'])

    def test_resolver_gracefully_handles_cache_miss_due_to_update_failure(self):
        """
        Test that if the cache is invalidated due to a failed update, the resolver gracefully fetches data from the database.
        """
        # Define the variables
        variables = {
            'startDate': '2015-01-01',
            'endDate': '2021-01-01',
            'dataPoints': 10
        }
        
        # Simulate a cache miss by ensuring the cache is empty
        last_updated_timestamp = int(max(
            self.gdp_series.last_updated.timestamp(),
            self.inflation_series.last_updated.timestamp()
        ))
        cache_key = f"quadrant_data_{variables['startDate']}_{variables['endDate']}_{variables['dataPoints']}_{int(last_updated_timestamp)}"
        cache.delete(cache_key)

        # Define the GraphQL query
        query = '''
            query ($startDate: Date, $endDate: Date, $dataPoints: Int) {
                quadrantData(startDate: $startDate, endDate: $endDate, dataPoints: $dataPoints) {
                    date
                    gdpGrowth
                    inflationGrowth
                }
            }
        '''

        # Execute the GraphQL query
        response = self.query(
            query,
            variables=variables
        )

        # Check the response status code
        print(response.content.decode('utf-8'))
        self.assertEqual(response.status_code, 200, msg=f"Expected status code 200, got {response.status_code}")

        # Parse the JSON response
        content = json.loads(response.content.decode('utf-8'))

        # Check for errors
        self.assertNotIn('errors', content, msg=f"GraphQL errors occurred: {content.get('errors')}")

        # Verify data is fetched from the database and cached
        fetched_data = content['data']['quadrantData']
        self.assertEqual(len(fetched_data), 4)
        # Additional assertions can be added here based on expected values

        # Verify cache is populated
        cached_data = cache.get(cache_key)
        self.assertIsNotNone(cached_data, msg="Data was not cached after cache miss.")
        # Ensure cached data matches fetched data
        self.assertEqual(len(cached_data), 4)