from django.contrib.contenttypes.fields import GenericRelation
from .data_point import DataPoint
from .data_series import DataSeries

class CorporateBondSpread(DataSeries):
    """
    A model to represent Corporate Bond Spreads data.
    """
    data_points = GenericRelation(DataPoint)