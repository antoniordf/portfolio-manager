from django.contrib.contenttypes.fields import GenericRelation
from .data_series import DataSeries
from .data_point import DataPoint

class TreasuryYield(DataSeries):
    """
    A model to represent US Treasury Yield data.
    """
    data_points = GenericRelation(DataPoint)