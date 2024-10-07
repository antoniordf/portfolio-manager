from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from dashboard.models import FedFundsRate

# Class-based view for Fed Funds Rate metadata
class FedFundsRateMetadataView(APIView):
    def get(self, request):
        real_gdp_metadata = FedFundsRate.objects.all()
        data = list(real_gdp_metadata.values())
        return Response(data)


# Class-based view for Fed Funds Rate data points by series_id
class FedFundsRateDataPointsView(APIView):
    def get(self, request, series_id):
        try:
            # Retrieve the specific Real GDP series by series_id
            real_gdp_series = FedFundsRate.objects.get(series_id=series_id)
            # Retrieve the associated data points, ordered by date as per the Meta class in DataPoint
            data_points = real_gdp_series.data_points.all()
            data = list(data_points.values())
            return Response(data)
        except FedFundsRate.DoesNotExist:
            return Response({'error': 'FedFundsRate series not found'}, status=status.HTTP_404_NOT_FOUND)