from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from dashboard.models import RealGDP, NominalInflation
from dashboard.serializers import QuadrantDataPointSerializer
from datetime import datetime

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

            # Align Inflation data to GDP dates
            inflation_df_aligned = inflation_df.reindex(gdp_df.index, method='nearest')

            # Ensure the indices (dates) are the same
            common_dates = gdp_df.index.intersection(inflation_df_aligned.index)

            # Apply date range filtering
            if start_date:
                common_dates = [date for date in common_dates if date.date() >= start_date]
            if end_date:
                common_dates = [date for date in common_dates if date.date() <= end_date]

            # Prepare the data
            data = []
            for date in common_dates[-data_points:]:  # Get the latest data points
                gdp_rate_of_change = gdp_df.loc[date, 'rate_of_change']
                inflation_rate_of_change = inflation_df_aligned.loc[date, 'rate_of_change']

                data.append({
                    'date': date.date(),  # Convert to date object
                    'gdp_growth': gdp_rate_of_change * 100,          # Convert to percentage
                    'inflation_growth': inflation_rate_of_change * 100,  # Convert to percentage
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
        
    def synchronize_and_merge_returns(self, real_gdp_returns, inflation_returns, start_date=None, end_date=None):
        # Convert lists of tuples to dictionaries for quick access
        gdp_returns_dict = dict(real_gdp_returns)
        inflation_returns_dict = dict(inflation_returns)

        # Get the intersection of dates (quarterly dates)
        dates = sorted(set(gdp_returns_dict.keys()) & set(inflation_returns_dict.keys()))

        # Apply date range filtering
        if start_date:
            dates = [date for date in dates if date >= start_date]
        if end_date:
            dates = [date for date in dates if date <= end_date]

        data = []
        for date in dates:
            gdp_growth = gdp_returns_dict[date]
            inflation_growth = inflation_returns_dict[date]

            data.append({
                'date': date.date(),  # Convert to date object
                'gdp_growth': gdp_growth * 100,          # Convert to percentage
                'inflation_growth': inflation_growth * 100,  # Convert to percentage
            })

        return data
