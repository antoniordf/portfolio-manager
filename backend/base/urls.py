from django.urls import path
from .views.real_gdp_views import real_gdp_metadata, real_gdp_data_points

app_name = 'base'

urlpatterns = [
    path('real_gdp_metadata/', real_gdp_metadata, name='real_gdp_metadata'),
    path('real_gdp_data_points/<int:series_id>/', real_gdp_data_points, name='real_gdp_data_points'),
]