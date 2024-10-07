from django.contrib.contenttypes.fields import GenericRelation
from .data_series import DataSeries
from .data_point import DataPoint

class RealGDP(DataSeries):
    """
    A model to represent real GDP data.
    """
    data_points = GenericRelation(DataPoint)