from django.contrib.contenttypes.fields import GenericRelation
from .data_series import DataSeries
from .data_point import DataPoint

class ISMServices(DataSeries):
    """
    A model to represent ISM Services data.
    """
    data_points = GenericRelation(DataPoint)