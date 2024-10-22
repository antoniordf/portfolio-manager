import graphene
from .types import RealGDPType, NominalInflationType, QuadrantDataPointType, FinancialDataPointType
from dashboard.models import RealGDP, NominalInflation, EconomicDataPoint, FinancialDataPoint
import pandas as pd
from django.core.cache import cache

class Query(graphene.ObjectType):
    real_gdp = graphene.Field(RealGDPType, id=graphene.Int(required=True))

    def resolve_real_gdp(self, info, id):
        try:
            return RealGDP.objects.get(id=id)
        except RealGDP.DoesNotExist:
            return None
        
    quadrant_data = graphene.List(
        QuadrantDataPointType,
        start_date=graphene.Date(),
        end_date=graphene.Date(),
        data_points=graphene.Int(default_value=15)
    )

    def resolve_quadrant_data(self, info, start_date=None, end_date=None, data_points=15):
        try:
            # Fetch data series for real GDP and inflation
            real_gdp_series = RealGDP.objects.get(series_id='GDPC1')
            inflation_series = NominalInflation.objects.get(series_id='CPIAUCSL')

            # Determine the last updated timestamp
            last_updated_real_gdp = real_gdp_series.last_updated
            last_updated_inflation = inflation_series.last_updated
            last_updated = max(last_updated_real_gdp, last_updated_inflation)
            last_updated_timestamp = int(last_updated.timestamp())

            # Construct the cache key
            cache_key = f'quadrant_data_{start_date}_{end_date}_{data_points}_{last_updated_timestamp}'

            # Attempt to retrieve from cache
            cached_data = cache.get(cache_key)
            if cached_data:
                return cached_data

            # Fetch related data points
            gdp_data_points = real_gdp_series.economic_data_points.all()
            inflation_data_points = inflation_series.economic_data_points.all()

            # Calculate YoY changes and rates of change for GDP
            gdp_df = EconomicDataPoint.calculate_returns(
                gdp_data_points,
                data_frequency=real_gdp_series.frequency
            )

            # Calculate YoY changes and rates of change for Inflation
            inflation_df = EconomicDataPoint.calculate_returns(
                inflation_data_points,
                data_frequency=inflation_series.frequency
            )

            # Resample inflation data to quarterly frequency
            inflation_df_quarterly = inflation_df.resample('QS').first()

            # Align the inflation data with GDP data
            inflation_df_aligned = inflation_df_quarterly.reindex(gdp_df.index)

            # Drop any NaN values resulting from reindexing
            inflation_df_aligned.dropna(inplace=True)
            gdp_df = gdp_df.loc[inflation_df_aligned.index]

            # Recalculate rate_of_change for both GDP and Inflation over one period (quarter)
            gdp_df['rate_of_change'] = gdp_df['yoy_change'] - gdp_df['yoy_change'].shift(1)
            inflation_df_aligned['rate_of_change'] = inflation_df_aligned['yoy_change'] - inflation_df_aligned['yoy_change'].shift(1)

            # Drop NaN values resulting from shift
            gdp_df.dropna(inplace=True)
            inflation_df_aligned.dropna(inplace=True)

            # Ensure both DataFrames have the same indices after dropping NaNs
            common_dates = gdp_df.index.intersection(inflation_df_aligned.index)
            gdp_df = gdp_df.loc[common_dates]
            inflation_df_aligned = inflation_df_aligned.loc[common_dates]

            # Combine the DataFrames
            combined_df = pd.concat([gdp_df, inflation_df_aligned], axis=1, keys=['gdp', 'inflation'])

            # Apply date range filtering
            if start_date:
                combined_df = combined_df[combined_df.index.date >= start_date]
            if end_date:
                combined_df = combined_df[combined_df.index.date <= end_date]

            # Limit the number of data points to the latest entries
            combined_df = combined_df.tail(data_points)

            # Prepare the data
            data = []
            for date, row in combined_df.iterrows():
                data.append(
                    QuadrantDataPointType(
                        date=date.date(),
                        gdp_growth=round(row[('gdp', 'rate_of_change')] * 100, 4),
                        inflation_growth=round(row[('inflation', 'rate_of_change')] * 100, 4),
                    )
                )

            # Cache the data with no timeout, since cache key includes last_updated
            cache.set(cache_key, data, timeout=None)
            
            return data

        except (RealGDP.DoesNotExist, NominalInflation.DoesNotExist):
            return []
        except ValueError as e:
            return []
        
    stock_time_series = graphene.List(
        FinancialDataPointType,
        series_id=graphene.String(required=True),
        start_date=graphene.Date(),
        end_date=graphene.Date(),
    )

    def resolve_stock_time_series(self, info, series_id, start_date=None, end_date=None):
        data = FinancialDataPoint.objects.filter(series__series_id=series_id)
        if start_date:
            data = data.filter(date__gte=start_date)
        if end_date:
            data = data.filter(date__lte=end_date)
        return data.order_by('date')
    