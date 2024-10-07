from django.contrib.contenttypes.fields import GenericRelation
from .data_series import DataSeries
from .data_point import DataPoint

class RealInterestRate(DataSeries):
    """
    A model to represent Real Interest Rate data.
    """
    data_points = GenericRelation(DataPoint)