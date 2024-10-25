from graphene_django.utils.testing import GraphQLTestCase
from django.core.cache import cache
from django.utils import timezone
from dashboard.models import EconomicDataPoint, RealGDP, NominalInflation
from config.schema import schema
from dateutil.relativedelta import relativedelta
import datetime
import json
import pandas as pd
import time

class QuadrantDataChartCacheTests(GraphQLTestCase):
    GRAPHQL_SCHEMA = schema
    GRAPHQL_URL = '/graphql/'

    @classmethod
    def setUpTestData(cls):
        # Create a DataSeries instance
        cls.gdp_series = RealGDP.objects.create(
            name="Gross Domestic Product",
            series_id="GDPC1",
            observation_start=datetime.date(2015, 1, 1),
            observation_end=datetime.date(2021, 1, 1),
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
            observation_start=datetime.date(2015, 1, 1),
            observation_end=datetime.date(2021, 1, 1),
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

        # Explicit GDP data points (quarterly)
        gdp_data = [
            {'date': datetime.date(2015, 1, 1), 'value': 18000},
            {'date': datetime.date(2015, 4, 1), 'value': 18250},
            {'date': datetime.date(2015, 7, 1), 'value': 18500},
            {'date': datetime.date(2015, 10, 1), 'value': 18750},
            {'date': datetime.date(2016, 1, 1), 'value': 19000},
            {'date': datetime.date(2016, 4, 1), 'value': 19250},
            {'date': datetime.date(2016, 7, 1), 'value': 19500},
            {'date': datetime.date(2016, 10, 1), 'value': 19750},
            {'date': datetime.date(2017, 1, 1), 'value': 20000},
            {'date': datetime.date(2017, 4, 1), 'value': 20250},
            {'date': datetime.date(2017, 7, 1), 'value': 20500},
            {'date': datetime.date(2017, 10, 1), 'value': 20750},
            {'date': datetime.date(2018, 1, 1), 'value': 21000},
            {'date': datetime.date(2018, 4, 1), 'value': 21250},
            {'date': datetime.date(2018, 7, 1), 'value': 21500},
            {'date': datetime.date(2018, 10, 1), 'value': 21750},
            {'date': datetime.date(2019, 1, 1), 'value': 22000},
            {'date': datetime.date(2019, 4, 1), 'value': 22250},  # This is the data point we'll update
            {'date': datetime.date(2019, 7, 1), 'value': 22500},
            {'date': datetime.date(2019, 10, 1), 'value': 22750},
        ]

        # Create GDP EconomicDataPoints at quarterly dates starting from 2015-01-01
        for data_point in gdp_data:
            EconomicDataPoint.objects.create(
                series=cls.gdp_series,
                date=data_point['date'],
                value=data_point['value']
            )

        # Explicit Inflation data points (monthly)
        inflation_data = []
        start_inflation_date = datetime.date(2015, 1, 1)
        for i in range(60):  # 5 years of monthly data
            date = start_inflation_date + relativedelta(months=i)
            value = 100 + i * 0.5
            inflation_data.append({'date': date, 'value': value})

        # Create Inflation EconomicDataPoints
        for data_point in inflation_data:
            EconomicDataPoint.objects.create(
                series=cls.inflation_series,
                date=data_point['date'],
                value=data_point['value']
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
            'startDate': '2015-01-01',
            'endDate': '2021-01-01',
            'dataPoints': 20
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
        self.assertEqual(response.status_code, 200, msg=f"Expected status code 200, got {response.status_code}")

        # Parse the JSON response
        content = json.loads(response.content.decode('utf-8'))

        # Check for errors in the response
        self.assertNotIn('errors', content, msg=f"GraphQL errors occurred: {content.get('errors')}")

        # Verify data is returned correctly
        self.assertIn('data', content, msg="Response does not contain 'data'")
        self.assertIn('quadrantData', content['data'], msg="Response data does not contain 'quadrantData'")
        # When calculating YoY changes on quarterly GDP data, we lose 4 data points. 
        # We lose one additional data point when calculating the rate of change.
        self.assertEqual(len(content['data']['quadrantData']), variables['dataPoints'] - 5)

        # Verify that data is now cached
        cached_data = cache.get(cache_key)
        self.assertIsNotNone(cached_data, msg="Data was not cached after cache miss.")
        self.assertEqual(len(cached_data), len(content['data']['quadrantData']))

    def test_cache_hit_returns_cached_data(self):
        """
        Test that a cache hit returns data from the cache without querying the database.
        """
        # Define the variables
        variables = {
            'startDate': '2015-01-01',
            'endDate': '2021-01-01',
            'dataPoints': 20
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
            'dataPoints': 20
        }

        # Initial cache population
        initial_last_updated_timestamp = int(max(
            self.gdp_series.last_updated.timestamp(),
            self.inflation_series.last_updated.timestamp()
        ))
        initial_cache_key = f"quadrant_data_{variables['startDate']}_{variables['endDate']}_{variables['dataPoints']}_{initial_last_updated_timestamp}"
        
        # Fetch GDP and Inflation data points
        gdp_data_points = EconomicDataPoint.objects.filter(series=self.gdp_series).order_by('date')
        inflation_data_points = EconomicDataPoint.objects.filter(series=self.inflation_series).order_by('date')

        #Create data frames
        gdp_df = pd.DataFrame.from_records(
            gdp_data_points.values('date', 'value'),
            index='date'
        )
        inflation_df = pd.DataFrame.from_records(
            inflation_data_points.values('date', 'value'),
            index='date'
        )

        # Convert index to datetime
        gdp_df.index = pd.to_datetime(gdp_df.index)
        inflation_df.index = pd.to_datetime(inflation_df.index)

        # Calculate YoY changes
        gdp_df['yoy_change'] = gdp_df['value'].pct_change(periods=4)
        inflation_df['yoy_change'] = inflation_df['value'].pct_change(periods=12)

        # Calculate rates of change
        gdp_df['rate_of_change'] = gdp_df['yoy_change'] - gdp_df['yoy_change'].shift(1)
        inflation_df['rate_of_change'] = inflation_df['yoy_change'] - inflation_df['yoy_change'].shift(1)

        # Drop NaN values resulting from calculations
        gdp_df.dropna(inplace=True)
        inflation_df.dropna(inplace=True)

        # Resample inflation data to quarterly frequency
        inflation_df_quarterly = inflation_df.resample('QS').first()

        # Align the data
        combined_df = pd.concat([gdp_df, inflation_df_quarterly], axis=1, keys=['gdp', 'inflation'])
        combined_df.dropna(inplace=True)

        # Flatten the MultiIndex columns
        combined_df.columns = ['_'.join(col) for col in combined_df.columns]

        # Apply date range filtering
        if variables['startDate']:
            combined_df = combined_df[combined_df.index.date >= datetime.datetime.strptime(variables['startDate'], '%Y-%m-%d').date()]
        if variables['endDate']:
            combined_df = combined_df[combined_df.index.date <= datetime.datetime.strptime(variables['endDate'], '%Y-%m-%d').date()]

        # Limit the number of data points to the latest entries
        combined_df = combined_df.tail(variables['dataPoints'])

        # Prepare the initial cached data with calculated values
        initial_cached_data = []
        for date, row in combined_df.iterrows():
            initial_cached_data.append({
                'date': date.date().isoformat(),
                'gdp_growth': round(row['gdp_rate_of_change'] * 100, 4),
                'inflationGrowth': round(row['inflation_rate_of_change'] * 100, 4),
            })
        cache.set(initial_cache_key, initial_cached_data, timeout=None)

        # Update an EconomicDataPoint
        updated_data_point = EconomicDataPoint.objects.get(series=self.gdp_series, date=datetime.date(2019, 4, 1))
        updated_data_point.value += 1000  # Example update
        updated_data_point.save()

        # After updating and saving the data point
        time.sleep(1)  # Sleep for 1 second

        # Manually update the last_updated timestamp
        self.gdp_series.last_updated = timezone.now()
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
        self.assertEqual(len(new_cached_data), len(initial_cached_data))

        # Find the index of the updated data point in new_cached_data
        updated_date_iso = updated_data_point.date.isoformat()
        updated_index = next(
            (index for (index, d) in enumerate(new_cached_data) if d['date'] == updated_date_iso),
            None
        )
        self.assertIsNotNone(updated_index, msg="Updated data point not found in the new cached data.")

        # Verify that the updated value is reflected
        updated_gdp_growth = new_cached_data[updated_index]['gdpGrowth']

        # Recalculate expected_gdp_growth after the update
        # Re-fetch data to reflect the updated value
        gdp_data_points_updated = EconomicDataPoint.objects.filter(series=self.gdp_series).order_by('date')
        gdp_df_updated = pd.DataFrame.from_records(
            gdp_data_points_updated.values('date', 'value'),
            index='date'
        )
        gdp_df_updated.index = pd.to_datetime(gdp_df_updated.index)
        gdp_df_updated['yoy_change'] = gdp_df_updated['value'].pct_change(periods=4)
        gdp_df_updated['rate_of_change'] = gdp_df_updated['yoy_change'] - gdp_df_updated['yoy_change'].shift(1)
        gdp_df_updated.dropna(inplace=True)

        # Get the updated rate_of_change for the updated_date
        expected_gdp_growth = round(
            gdp_df_updated.loc[pd.Timestamp(updated_data_point.date)]['rate_of_change'] * 100, 4
        )

        self.assertAlmostEqual(updated_gdp_growth, expected_gdp_growth, places=2)

        # Verify that the old cache still exists
        old_cached_data = cache.get(initial_cache_key)
        self.assertIsNotNone(old_cached_data, msg="Old cache key was unexpectedly deleted.")
        
        # Verify that the old GDP growth value is different
        old_updated_index = next(
            (index for (index, d) in enumerate(initial_cached_data) if d['date'] == updated_date_iso),
            None
        )
        old_gdp_growth = initial_cached_data[old_updated_index]['gdp_growth']
        self.assertNotEqual(updated_gdp_growth, old_gdp_growth, msg="GDP growth should have changed after data update.")

    def test_resolver_gracefully_handles_cache_miss_due_to_update_failure(self):
        """
        Test that if the cache is invalidated due to a failed update, the resolver gracefully fetches data from the database.
        """
        # Define the variables
        variables = {
            'startDate': '2015-01-01',
            'endDate': '2021-01-01',
            'dataPoints': 20
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
        self.assertEqual(response.status_code, 200, msg=f"Expected status code 200, got {response.status_code}")

        # Parse the JSON response
        content = json.loads(response.content.decode('utf-8'))

        # Check for errors
        self.assertNotIn('errors', content, msg=f"GraphQL errors occurred: {content.get('errors')}")

        # Verify data is fetched from the database and cached
        fetched_data = content['data']['quadrantData']
        # When calculating YoY changes on quarterly GDP data, we lose 4 data points. 
        # We lose one additional data point when calculating the rate of change.
        self.assertEqual(len(fetched_data), variables['dataPoints'] - 5)

        # Verify cache is populated
        cached_data = cache.get(cache_key)
        self.assertIsNotNone(cached_data, msg="Data was not cached after cache miss.")
        # Ensure cached data matches fetched data
        # When calculating YoY changes on quarterly GDP data, we lose 4 data points. 
        # We lose one additional data point when calculating the rate of change.
        self.assertEqual(len(cached_data), variables['dataPoints'] - 5)