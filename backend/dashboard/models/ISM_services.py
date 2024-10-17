from django.db import models
from .economic_data_point import EconomicDataPoint
from .data_series import DataSeries

class ISMServices(DataSeries):
    """
    A model to represent ISM Services data.
    """

    def save(self, *args, **kwargs):
        self.data_type = 'economic'
        super().save(*args, **kwargs)