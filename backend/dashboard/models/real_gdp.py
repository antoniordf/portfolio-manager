from django.db import models
from .data_series import DataSeries
from .economic_data_point import EconomicDataPoint


class RealGDP(DataSeries):
    """
    A model to represent real GDP data.
    """

    def save(self, *args, **kwargs):
        self.data_type = 'economic'
        super().save(*args, **kwargs)