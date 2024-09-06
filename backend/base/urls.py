from django.urls import path
from .views import RealGDPMetadataView, RealGDPDataPointsView

app_name = 'base'

urlpatterns = [
    path('real_gdp_metadata/', RealGDPMetadataView.as_view(), name='real_gdp_metadata'),
    path('real_gdp_data_points/<str:series_id>/', RealGDPDataPointsView.as_view(), name='real_gdp_data_points'),
]