from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from dashboard.models import NominalInflation

# Class-based view for Nominal Inflation metadata
class NominalInflationMetadataView(APIView):
    def get(self, request):
        nominal_inflation_metadata = NominalInflation.objects.all()
        data = list(nominal_inflation_metadata.values())
        return Response(data)


# Class-based view for Nominal Inflation data points by series_id
class NominalInflationDataPointsView(APIView):
    def get(self, request, series_id):
        try:
            # Retrieve the specific Nominal Inflation series by series_id
            nominal_inflation_series = NominalInflation.objects.get(series_id=series_id)
            # Retrieve the associated data points, ordered by date as per the Meta class in DataPoint
            data_points = nominal_inflation_series.data_points.all()
            data = list(data_points.values())
            return Response(data)
        except NominalInflation.DoesNotExist:
            return Response({'error': 'Nominal Inflation series not found'}, status=status.HTTP_404_NOT_FOUND)