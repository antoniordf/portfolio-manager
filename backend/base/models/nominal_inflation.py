from django.contrib.contenttypes.fields import GenericRelation
from .data_series import DataSeries
from .data_point import DataPoint

class NominalInflation(DataSeries):
    """
    A model to represent Nominal Inflation data.
    """
    data_points = GenericRelation(DataPoint)