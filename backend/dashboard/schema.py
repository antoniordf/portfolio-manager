import graphene
from graphene_django import DjangoObjectType
from .models.real_gdp import RealGDP
from .models.nominal_inflation import NominalInflation
from .models.data_point import DataPoint
from datetime import datetime
import pandas as pd

# Define a GraphQL type for DataPoint
class DataPointType(DjangoObjectType):
    class Meta:
        model = DataPoint
        fields = ('id', 'date', 'value')

# Define a GraphQL type for RealGDP
class RealGDPType(DjangoObjectType):
    data_points = graphene.List(DataPointType)

    class Meta:
        model = RealGDP
        fields = (
            'id',
            'name',
            'series_id',
            'observation_start',
            'observation_end',
            'frequency',
            'units',
            'seasonal_adjustment',
            'last_updated',
            'notes',
            'metadata',
        )

    # Resolve data_points field
    def resolve_data_points(self, info):
        return self.data_points.all().order_by('date')

# Define a GraphQL type for QuadrantDataPoint 
class QuadrantDataPointType(graphene.ObjectType):
    date = graphene.Date()
    gdp_growth = graphene.Float()
    inflation_growth = graphene.Float()

# Define the Query class for the dashboard app
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

            # Calculate YoY changes and rates of change for GDP
            gdp_df = real_gdp_series.calculate_returns(
                data_frequency=real_gdp_series.frequency
            )

            # Calculate YoY changes and rates of change for Inflation
            inflation_df = inflation_series.calculate_returns(
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

            return data

        except (RealGDP.DoesNotExist, NominalInflation.DoesNotExist):
            return []
        except ValueError as e:
            return []
    