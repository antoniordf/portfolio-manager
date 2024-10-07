from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from dashboard.models import CorporateBondSpread

# Class-based view for Corporate Bond Spread metadata
class CorporateBondSpreadMetadataView(APIView):
    def get(self, request):
        corporate_bond_spread_metadata = CorporateBondSpread.objects.all()
        data = list(corporate_bond_spread_metadata.values())
        return Response(data)


# Class-based view for Corporate Bond Spread data points by series_id
class CorporateBondSpreadDataPointsView(APIView):
    def get(self, request, series_id):
        try:
            # Retrieve the specific Real GDP series by series_id
            corporate_bond_spread_series = CorporateBondSpread.objects.get(series_id=series_id)
            # Retrieve the associated data points, ordered by date as per the Meta class in DataPoint
            data_points = corporate_bond_spread_series.data_points.all()
            data = list(data_points.values())
            return Response(data)
        except CorporateBondSpread.DoesNotExist:
            return Response({'error': 'CorporateBondSpread series not found'}, status=status.HTTP_404_NOT_FOUND)