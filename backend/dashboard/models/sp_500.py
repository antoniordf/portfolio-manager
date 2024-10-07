from django.contrib.contenttypes.fields import GenericRelation
from .data_series import DataSeries
from .data_point import DataPoint

class Sp500(DataSeries):
    """
    A model to represent S&P 500 data.
    """
    data_points = GenericRelation(DataPoint)