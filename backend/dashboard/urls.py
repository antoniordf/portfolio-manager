from django.urls import path
from .views import RealGDPMetadataView, RealGDPDataPointsView, CorporateBondSpreadMetadataView, CorporateBondSpreadDataPointsView
from .views import FedFundsRateMetadataView, FedFundsRateDataPointsView, NominalInflationMetadataView, NominalInflationDataPointsView, QuadrantDataView

app_name = 'dashboard'

urlpatterns = [
    path('real_gdp_metadata/', RealGDPMetadataView.as_view(), name='real_gdp_metadata'),
    path('real_gdp_data_points/<str:series_id>/', RealGDPDataPointsView.as_view(), name='real_gdp_data_points'),
    path('corporate_bond_spread_metadata/', CorporateBondSpreadMetadataView.as_view(), name='corporate_bond_spread_metadata'),
    path('corporate_bond_spread_data_points/<str:series_id>/', CorporateBondSpreadDataPointsView.as_view(), name='corporate_bond_spread_data_points'),
    path('fed_funds_rate_metadata/', FedFundsRateMetadataView.as_view(), name='fed_funds_rate_metadata'),
    path('fed_funds_rate_data_points/<str:series_id>/', FedFundsRateDataPointsView.as_view(), name='fed_funds_rate_data_points'),
    path('nominal_inflation_metadata/', NominalInflationMetadataView.as_view(), name='nominal_inflation_metadata'),
    path('nominal_inflation_data_points/<str:series_id>/', NominalInflationDataPointsView.as_view(), name='nominal_inflation_data_points'),
    path('quadrant_data/', QuadrantDataView.as_view(), name='quadrant_data'),
]