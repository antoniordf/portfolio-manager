from django.contrib.contenttypes.fields import GenericRelation
from .data_series import DataSeries  
from .data_point import DataPoint

class ConcreteDataSeries(DataSeries):
    data_points = GenericRelation(DataPoint)