from django.db import models
from .financial_data_point import FinancialDataPoint
from .data_series import DataSeries

class VIX(DataSeries):
    """
    A model to represent VIX data.
    """

    def save(self, *args, **kwargs):
        self.data_type = 'financial'
        super().save(*args, **kwargs)