from django.contrib.contenttypes.fields import GenericRelation
from .data_series import DataSeries
from .data_point import DataPoint

class VIX(DataSeries):
    """
    A model to represent VIX data.
    """
    data_points = GenericRelation(DataPoint)