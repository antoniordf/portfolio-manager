import csv
from django.db import models
from dateutil.relativedelta import relativedelta
from dateutil.relativedelta import relativedelta
from datetime import timedelta
from .data_point import DataPoint
import numpy as np
from scipy import stats as scipy_stats
import pandas as pd

# Parent class: Represents the metadata and methods for the time series
class DataSeries(models.Model):
    name = models.CharField(max_length=100)
    series_id = models.CharField(max_length=50, unique=True)
    observation_start = models.DateField()
    observation_end = models.DateField()
    frequency = models.CharField(max_length=50)
    units = models.CharField(max_length=100)
    seasonal_adjustment = models.CharField(max_length=100)
    last_updated = models.DateTimeField(auto_now=True)
    notes = models.TextField(blank=True)
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        abstract = True

    def __str__(self):
        return self.name
    
    def get_metadata(self) -> dict:
        """
        Retrieve and return the metadata associated with the data series.
        """
        metadata = {
            'name': self.name,
            'series_id': self.series_id,
            'observation_start': self.observation_start,
            'observation_end': self.observation_end,
            'frequency': self.frequency,
            'units': self.units,
            'seasonal_adjustment': self.seasonal_adjustment,
            'last_updated': self.last_updated,
            'notes': self.notes,
        }
        
        # Merge in additional metadata stored in the JSONField
        metadata.update(self.metadata)
        
        return metadata

    def get_time_series(self) -> list:
        # Returns the entire time series as a sorted list of tuples (date, value)
        return sorted([(dp.date, dp.value) for dp in self.data_points.all()], key=lambda x: x[0])

    def calculate_returns(self, data_frequency) -> dict:
        """
        Calculate the returns for the time series data based on the specified period.
        Parameters:
        - data_frequency (str): The frequency of the underlying data (e.g., "Daily", "Monthly", "Yearly").
        - return_period (str): The period over which to calculate returns (e.g., "daily", "weekly", "monthly", "yearly").
        """
        time_series = self.get_time_series()
        df = pd.DataFrame(time_series, columns=['date', 'value'])

        # Convert 'date' column to datetime
        df['date'] = pd.to_datetime(df['date'])

        # Set 'date' as the index
        df.set_index('date', inplace=True)
        df.sort_index(inplace=True)  # Ensure dates are in order

        # Calculate YoY growth rates
        if data_frequency == "Monthly":
            # Calculate YoY growth rates (12 months apart)
            yoy_returns = df.pct_change(periods=12)
        elif data_frequency == "Quarterly":
            # Calculate YoY growth rates (4 quarters apart)
            yoy_returns = df.pct_change(periods=4)
        else:
            raise ValueError(f"Unsupported data frequency: {data_frequency}")

        # Calculate the rate of change of YoY growth rates
        delta_returns = yoy_returns - yoy_returns.shift(periods=1)

        # Combine into a single DataFrame
        result_df = pd.DataFrame({
            'value': df['value'],
            'yoy_change': yoy_returns['value'],
            'rate_of_change': delta_returns['value']
        })

        # Drop NaN values resulting from the calculations
        result_df.dropna(inplace=True)

        return result_df
    
    def get_return_series(self, period) -> list:
        """
        Get the return series for the time series data.
        """
        return_series = self.calculate_returns(period)
        return sorted([(date, value) for date, value in return_series.items()], key=lambda x: x[0])
    
    def add_data_point(self, date, value):
        # Add a new data point to the time series
        DataPoint.objects.create(date=date, value=value, series=self)

    def update_data_point(self, date, new_value) -> None:
        try:
            # The self.data_points is a reverse relation queryset to DataPoint objects
            data_point = self.data_points.get(date=date)
            data_point.value = new_value
            data_point.save()
        except DataPoint.DoesNotExist:
            raise ValueError(f"No data point exists for the date {date}.")
        
    def count_observations(self) -> int:
        return self.data_points.count()
    
    def data_point_exists(self, date) -> bool:
        return self.data_points.filter(date=date).exists()
    
    def get_latest_data_point(self) -> DataPoint:
        return self.data_points.latest("date")
    
    def calculate_statistics(self) -> dict:
        # Extract the time series values
        values = self.get_time_series_values()

        # Handle the case where there are no data points
        if not values:
            return {}

        # Calculate descriptive statistics
        statistics = {
            'mean': np.mean(values),
            'median': np.median(values),
            'mode': scipy_stats.mode(values)[0][0] if len(values) > 1 else None,
            'standard_deviation': np.std(values, ddof=1),  # Sample standard deviation
            'variance': np.var(values, ddof=1),  # Sample variance
            'skewness': scipy_stats.skew(values),
            'kurtosis': scipy_stats.kurtosis(values),
            'minimum': np.min(values),
            'maximum': np.max(values),
            'range': np.ptp(values),  # Range (max - min)
            'count': len(values),
            'sum': np.sum(values),
            '25th_percentile': np.percentile(values, 25),
            '50th_percentile': np.percentile(values, 50),  # same as median
            '75th_percentile': np.percentile(values, 75),
            'interquartile_range': scipy_stats.iqr(values),  # IQR
        }

        return statistics

    def export_to_csv(self, filename) -> None:
        """
        Export the time series data to a CSV file.
        """
        time_series = self.get_time_series()

        # Open the file for writing
        with open(filename, mode="w", newline='') as file:
            writer = csv.writer(file)
            
            # Write the header
            writer.writerow(["date", "value"])
            
            # Write the data rows
            for date, value in time_series:
                writer.writerow([date, value])
        
