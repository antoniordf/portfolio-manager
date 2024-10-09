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
        return_period = request.query_params.get('return_period', 'quarterly')  # Default to 'quarterly' for GDP

        # Parse dates if provided
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        try:
            # Fetch data points for real GDP and inflation
            real_gdp_series = RealGDP.objects.get(series_id='GDPC1')
            inflation_series = NominalInflation.objects.get(series_id='CPIAUCSL')
            
            # Calculate returns (growth rates)
            real_gdp_returns = real_gdp_series.calculate_returns(
                data_frequency=real_gdp_series.frequency, return_period=return_period
            )
            inflation_returns = inflation_series.calculate_returns(
                data_frequency=inflation_series.frequency, return_period='monthly'
            )
            
            # Convert returns dictionaries to lists of tuples and sort by date
            real_gdp_returns = sorted(real_gdp_returns.items())
            inflation_returns = sorted(inflation_returns.items())

            # Synchronize dates between GDP and inflation returns
            data = self.synchronize_and_merge_returns(
                real_gdp_returns, inflation_returns, start_date, end_date
            )

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

        # Create a list of dates from the inflation data (monthly dates)
        dates = sorted(inflation_returns_dict.keys())

        # Apply date range filtering
        if start_date:
            dates = [date for date in dates if date >= start_date]
        if end_date:
            dates = [date for date in dates if date <= end_date]

        data = []
        for date in dates:
            # Find the corresponding GDP date (previous quarter)
            gdp_date = max([d for d in gdp_returns_dict.keys() if d <= date], default=None)
            if gdp_date is None:
                continue  # Skip if no GDP data is available before this date

            gdp_growth = gdp_returns_dict[gdp_date]
            inflation_growth = inflation_returns_dict[date]

            data.append({
                'date': date,
                'gdp_growth': gdp_growth * 100,          # Convert to percentage
                'inflation_growth': inflation_growth * 100,  # Convert to percentage
            })

        return data
