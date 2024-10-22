from django.db import models
import pandas as pd
from django.core.exceptions import ValidationError

class FinancialDataPoint(models.Model):
    series = models.ForeignKey(
        'dashboard.DataSeries',
        related_name='financial_data_points',
        on_delete=models.CASCADE
    )
    date = models.DateField()
    open = models.FloatField()
    high = models.FloatField()
    low = models.FloatField()
    close = models.FloatField()
    volume = models.BigIntegerField()

    class Meta:
        db_table = 'financial_data_point'
        unique_together = ('series', 'date')
        get_latest_by = 'date'

    def __str__(self):
        return f"{self.series.name} on {self.date}: Open={self.open}, Close={self.close}"
    
    def clean(self):
        if self.__class__.objects.filter(series=self.series, date=self.date).exists() and not self.pk:
            raise ValidationError(f"A data point for {self.date} in {self.series} already exists.")

    @classmethod
    def calculate_returns(cls, data_points, period='daily', price_type='close'):
        """
        Calculate returns for a queryset of FinancialDataPoint instances.
        """
        # Extract time series data
        time_series = [(dp.date, getattr(dp, price_type)) for dp in data_points]
        df = pd.DataFrame(time_series, columns=['date', 'price'])

        # Convert 'date' column to datetime and set as index
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df.sort_index(inplace=True)

        if period == 'daily':
            returns = df['price'].pct_change()
        elif period == 'weekly':
            returns = df['price'].pct_change(periods=5)
        elif period == 'monthly':
            returns = df['price'].pct_change(periods=21)
        else:
            raise ValueError(f"Unsupported period: {period}")

        # Combine into a single DataFrame
        result_df = pd.DataFrame({
            'price': df['price'],
            'returns': returns,
        })

        # Drop NaN values resulting from the calculations
        result_df.dropna(inplace=True)

        return result_df