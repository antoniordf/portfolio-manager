from django.db import models
from .economic_data_point import EconomicDataPoint
from .data_series import DataSeries

class FedFundsRate(DataSeries):
    """
    A model to represent Fed Funds Rate data.
    """

    def save(self, *args, **kwargs):
        self.data_type = 'economic'
        super().save(*args, **kwargs)