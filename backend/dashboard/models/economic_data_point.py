from django.db import models
import pandas as pd
from django.core.exceptions import ValidationError

class EconomicDataPoint(models.Model):
    series = models.ForeignKey(
        'dashboard.DataSeries',
        related_name='economic_data_points',
        on_delete=models.CASCADE
    )
    date = models.DateField()
    value = models.FloatField()

    class Meta:
        db_table = 'economic_data_point'
        unique_together = ('series', 'date')
        get_latest_by = 'date'

    def __str__(self):
        return f"{self.series.name} on {self.date}: {self.value}"
    
    def clean(self):
        if self.__class__.objects.filter(series=self.series, date=self.date).exists() and not self.pk:
            raise ValidationError(f"A data point for {self.date} in {self.series} already exists.")

    @classmethod
    def calculate_returns(cls, data_points, data_frequency):
        """
        Calculate returns for a queryset of EconomicDataPoint instances.
        """
        # Extract time series data
        time_series = [(dp.date, dp.value) for dp in data_points]
        df = pd.DataFrame(time_series, columns=['date', 'value'])

        # Convert 'date' column to datetime and set as index
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df.sort_index(inplace=True)

        # Calculate YoY growth rates
        if data_frequency == "Monthly":
            yoy_returns = df['value'].pct_change(periods=12)
        elif data_frequency == "Quarterly":
            yoy_returns = df['value'].pct_change(periods=4)
        else:
            raise ValueError(f"Unsupported data frequency: {data_frequency}")

        # Combine into a single DataFrame
        result_df = pd.DataFrame({
            'value': df['value'],
            'yoy_change': yoy_returns,
        })

        # Drop NaN values resulting from the calculations
        result_df.dropna(inplace=True)

        return result_df