from django.db import models
from .economic_data_point import EconomicDataPoint
from .data_series import DataSeries

class RealInterestRate(DataSeries):
    """
    A model to represent Real Interest Rate data.
    """

    def save(self, *args, **kwargs):
        self.data_type = 'economic'
        super().save(*args, **kwargs)