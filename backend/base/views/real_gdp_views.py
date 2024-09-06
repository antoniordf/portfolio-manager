from django.shortcuts import render
from base.models import RealGDP

def real_gdp_metadata(request):
    # Retrieve metadata for the Real GDP series
    real_gdp_metadata = RealGDP.objects.all()
    
    return render(request, 
                  'base/dashboard/real_gdp_metadata.html', 
                  {'real_gdp_metadata': real_gdp_metadata})
    

def real_gdp_data_points(request, series_id):
    # Retrieve the specific Real GDP series by series_id
    real_gdp_series = RealGDP.objects.get(series_id=series_id)
    
    # Retrieve the associated data points, ordered by date as per the Meta class in DataPoint
    data_points = real_gdp_series.data_points.all()

    return render(request, 
                  'base/dashboard/real_gdp_data_points.html', 
                  {'real_gdp_series': real_gdp_series, 'data_points': data_points})