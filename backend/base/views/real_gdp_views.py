from django.http import JsonResponse
from django.views import View
from base.models import RealGDP

# Class-based view for Real GDP metadata
class RealGDPMetadataView(View):
    def get(self, request):
        real_gdp_metadata = RealGDP.objects.all()
        data = list(real_gdp_metadata.values())
        return JsonResponse(data, safe=False)


# Class-based view for Real GDP data points by series_id
class RealGDPDataPointsView(View):
    def get(self, request, series_id):
        try:
            # Retrieve the specific Real GDP series by series_id
            real_gdp_series = RealGDP.objects.get(series_id=series_id)
            # Retrieve the associated data points, ordered by date as per the Meta class in DataPoint
            data_points = real_gdp_series.data_points.all()
            data = list(data_points.values())
            return JsonResponse(data, safe=False)
        except RealGDP.DoesNotExist:
            return JsonResponse({'error': 'RealGDP series not found'}, status=404)