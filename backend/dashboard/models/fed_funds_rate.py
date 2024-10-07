from django.contrib.contenttypes.fields import GenericRelation
from .data_series import DataSeries
from .data_point import DataPoint

class FedFundsRate(DataSeries):
    """
    A model to represent Fed Funds Rate data.
    """
    data_points = GenericRelation(DataPoint)