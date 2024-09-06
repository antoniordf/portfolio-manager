from django.urls import path
from .views import real_gdp_metadata

app_name = 'base'

urlpatterns = [
    path('real_gdp_metadata/', real_gdp_metadata, name='real_gdp_metadata'),
]