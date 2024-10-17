from django.db import models
from .financial_data_point import FinancialDataPoint
from .data_series import DataSeries

class Sp500(DataSeries):
    """
    A model to represent S&P 500 data.
    """

    def save(self, *args, **kwargs):
        self.data_type = 'financial'
        super().save(*args, **kwargs)