from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from dashboard.models import RealGDP, NominalInflation
from dashboard.serializers import QuadrantDataPointSerializer
from datetime import datetime
import pandas as pd

class QuadrantDataView(APIView):
    def get(self, request):
        # Optional: get date range from query parameters
        start_date = request.query_params.get('start_date')
        end_date = request.query_params.get('end_date')
        data_points = 25  # Default to 25 data points

        # Parse dates if provided
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

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
                data.append({
                    'date': date.date(),
                    'gdp_growth': round(row[('gdp', 'rate_of_change')] * 100, 4),
                    'inflation_growth': round(row[('inflation', 'rate_of_change')] * 100, 4),
                })

            # Use the serializer to validate and serialize the data
            serializer = QuadrantDataPointSerializer(data=data, many=True)
            if serializer.is_valid():
                return Response(serializer.data, status=status.HTTP_200_OK)
            else:
                return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        except (RealGDP.DoesNotExist, NominalInflation.DoesNotExist):
            return Response({'error': 'Data series not found'}, status=status.HTTP_404_NOT_FOUND)
        except ValueError as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)
        
