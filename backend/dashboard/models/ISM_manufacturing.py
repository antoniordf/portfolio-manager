from django.contrib.contenttypes.fields import GenericRelation
from .data_series import DataSeries
from .data_point import DataPoint

class ISMManufacturing(DataSeries):
    """
    A model to represent ISM Manufacturing data.
    """
    data_points = GenericRelation(DataPoint)