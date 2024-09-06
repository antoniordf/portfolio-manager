from django.http import JsonResponse
from base.models import RealGDP

def real_gdp_metadata(request):
    real_gdp_metadata = RealGDP.objects.all()
    data = list(real_gdp_metadata.values())
    return JsonResponse(data, safe=False)
    

def real_gdp_data_points(request, series_id):
    # Retrieve the specific Real GDP series by series_id
    real_gdp_series = RealGDP.objects.get(series_id=series_id)
    # Retrieve the associated data points, ordered by date as per the Meta class in DataPoint
    data_points = real_gdp_series.data_points.all()
    data = list(data_points.values())
    return JsonResponse(data, safe=False)