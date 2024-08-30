import csv
from django.db import models
from dateutil.relativedelta import relativedelta
from dateutil import relativedelta
from datetime import timedelta
from .data_point import DataPoint
import numpy as np
from scipy import stats as scipy_stats

# Parent class: Represents the metadata and methods for the time series
class DataSeries(models.Model):
    name = models.CharField(max_length=100)
    series_id = models.CharField(max_length=50, unique=True)
    observation_start = models.DateField()
    observation_end = models.DateField()
    frequency = models.CharField(max_length=50)
    units = models.CharField(max_length=100)
    seasonal_adjustment = models.CharField(max_length=100)
    last_updated = models.DateTimeField()
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

    def calculate_returns(self, data_frequency, return_period="daily") -> dict:
        """
        Calculate the returns for the time series data based on the specified period.
        Parameters:
        - data_frequency (str): The frequency of the underlying data (e.g., "Daily", "Monthly", "Yearly").
        - return_period (str): The period over which to calculate returns (e.g., "daily", "weekly", "monthly", "yearly").
        """
        time_series = self.get_time_series()
        
        # Convert time_series to a dictionary for quick access by date
        time_series_dict = {date: value for date, value in time_series}
        returns = {}

        def calculate_return_for_period(offset_func):
            """
            Helper function to calculate returns for a given period offset.
            """
            for i in range(1, len(time_series)):
                current_date, current_value = time_series[i]
                previous_date = offset_func(current_date)
                if previous_date in time_series_dict:
                    previous_value = time_series_dict[previous_date]
                    return_value = (current_value / previous_value) - 1
                    returns[current_date] = return_value

        # Determine the valid return periods based on the data frequency
        if data_frequency == "Daily":
            if return_period == "daily":
                calculate_return_for_period(lambda date: date - timedelta(days=1))
            elif return_period == "weekly":
                calculate_return_for_period(lambda date: date - timedelta(weeks=1))
            elif return_period == "monthly":
                calculate_return_for_period(lambda date: date - relativedelta(months=1))
            elif return_period == "quarterly":
                calculate_return_for_period(lambda date: date - relativedelta(months=3))
            elif return_period == "yearly":
                calculate_return_for_period(lambda date: date - relativedelta(years=1))
        elif data_frequency == "Monthly":
            if return_period == "monthly":
                calculate_return_for_period(lambda date: date - relativedelta(months=1))
            elif return_period == "quarterly":
                calculate_return_for_period(lambda date: date - relativedelta(months=3))
            elif return_period == "yearly":
                calculate_return_for_period(lambda date: date - relativedelta(years=1))
            else:
                raise ValueError(f"Cannot calculate {return_period} returns from monthly data.")
        elif data_frequency == "Quarterly":
            if return_period == "quarterly":
                calculate_return_for_period(lambda date: date - relativedelta(months=3))
            elif return_period == "yearly":
                calculate_return_for_period(lambda date: date - relativedelta(years=1))
            else:
                raise ValueError(f"Cannot calculate {return_period} returns from quarterly data.")
        elif data_frequency == "Yearly":
            if return_period == "yearly":
                calculate_return_for_period(lambda date: date - relativedelta(years=1))
            else:
                raise ValueError(f"Cannot calculate {return_period} returns from yearly data.")
        else:
            raise ValueError(f"Unsupported data frequency: {data_frequency}")

        # Return the dictionary of returns with dates as keys
        return returns
    
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
        
