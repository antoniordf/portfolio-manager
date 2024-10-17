import csv
from django.db import models
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
    last_updated = models.DateTimeField(auto_now=True)
    notes = models.TextField(blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    
    DATA_TYPES = (
        ('economic', 'Economic'),
        ('financial', 'Financial'),
    )
    data_type = models.CharField(max_length=10, choices=DATA_TYPES)


    def __str__(self) -> str:
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
        if self.data_type == 'economic':
            data_points = self.economic_data_points.all()
            return sorted([(dp.date, dp.value) for dp in data_points], key=lambda x: x[0])
        elif self.data_type == 'financial':
            data_points = self.financial_data_points.all()
            return sorted([(dp.date, dp.close) for dp in data_points], key=lambda x: x[0])
        else:
            return []
    
    def get_return_series(self, period) -> list:
        """
        Get the return series for the time series data.
        """
        if self.data_type == 'economic':
            from .economic_data_point import EconomicDataPoint
            data_points = self.economic_data_points.all()
            # Ensure data_points is ordered by date
            data_points = data_points.order_by('date')
            return_series = EconomicDataPoint.calculate_returns(data_points, self.frequency)
        elif self.data_type == 'financial':
            from .financial_data_point import FinancialDataPoint
            data_points = self.financial_data_points.all()
            # Ensure data_points is ordered by date
            data_points = data_points.order_by('date')
            return_series = FinancialDataPoint.calculate_returns(data_points, period)
        else:
            return []

        # Return sorted list of tuples (date, return_value)
        return sorted(return_series.items(), key=lambda x: x[0])
    
    def add_data_point(self, date, **kwargs) -> None:
        if self.data_type == 'economic':
            from .economic_data_point import EconomicDataPoint
            if 'value' not in kwargs:
                raise ValueError("Missing required argument 'value' for economic data point.")
            EconomicDataPoint.objects.create(series=self, date=date, value=kwargs['value'])
        elif self.data_type == 'financial':
            from .financial_data_point import FinancialDataPoint
            required_fields = ['open', 'high', 'low', 'close', 'volume']
            missing_fields = [field for field in required_fields if field not in kwargs]
            if missing_fields:
                raise ValueError(f"Missing required fields for financial data point: {', '.join(missing_fields)}")
            FinancialDataPoint.objects.create(
                series=self,
                date=date,
                open=kwargs.get('open'),
                high=kwargs.get('high'),
                low=kwargs.get('low'),
                close=kwargs.get('close'),
                volume=kwargs.get('volume'),
            )

    def update_data_point(self, date, **kwargs) -> None:
        if self.data_type == 'economic':
            from .economic_data_point import EconomicDataPoint
            try:
                data_point = self.economic_data_points.get(date=date)
                if 'value' not in kwargs:
                    raise ValueError("Missing required argument 'value' for economic data point.")
                data_point.value = kwargs['value']
                data_point.save()
            except EconomicDataPoint.DoesNotExist:
                raise ValueError(f"No data point exists for the date {date}.")
        elif self.data_type == 'financial':
            from .financial_data_point import FinancialDataPoint
            try:
                data_point = self.financial_data_points.get(date=date)
                for field in ['open', 'high', 'low', 'close', 'volume']:
                    if field in kwargs:
                        setattr(data_point, field, kwargs[field])
                data_point.save()
            except FinancialDataPoint.DoesNotExist:
                raise ValueError(f"No data point exists for the date {date}.")
            
    def count_observations(self) -> int:
        if self.data_type == 'economic':
            return self.economic_data_points.count()
        elif self.data_type == 'financial':
            return self.financial_data_points.count()
        else:
            return 0
    
    def data_point_exists(self, date) -> bool:
        if self.data_type == 'economic':
            return self.economic_data_points.filter(date=date).exists()
        elif self.data_type == 'financial':
            return self.financial_data_points.filter(date=date).exists()
        else:
            return False
    
    def get_latest_data_point(self):
        if self.data_type == 'economic':
            return self.economic_data_points.latest("date")
        elif self.data_type == 'financial':
            return self.financial_data_points.latest("date")
        else:
            return None
    
    def calculate_statistics(self) -> dict:
        if self.data_type == 'economic':
            values = [dp.value for dp in self.economic_data_points.all()]
        elif self.data_type == 'financial':
            values = [dp.close for dp in self.financial_data_points.all()]
        else:
            return {}

        # Handle the case where there are no data points
        if not values:
            return {}

        # Calculate Mode
        mode_result = scipy_stats.mode(values, nan_policy='omit')
        mode_value = mode_result.mode[0] if mode_result.count[0] > 0 else None

        # Calculate descriptive statistics
        statistics = {
            'mean': np.mean(values),
            'median': np.median(values),
            'mode': mode_value,
            'standard_deviation': np.std(values, ddof=1),
            'variance': np.var(values, ddof=1),
            'skewness': scipy_stats.skew(values),
            'kurtosis': scipy_stats.kurtosis(values),
            'minimum': np.min(values),
            'maximum': np.max(values),
            'range': np.ptp(values),
            'count': len(values),
            'sum': np.sum(values),
            '25th_percentile': np.percentile(values, 25),
            '50th_percentile': np.percentile(values, 50),
            '75th_percentile': np.percentile(values, 75),
            'interquartile_range': scipy_stats.iqr(values),
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
            
            if self.data_type == 'economic':
                # Write the header
                writer.writerow(["date", "value"])
                # Write the data rows
                for date, value in time_series:
                    writer.writerow([date, value])
            elif self.data_type == 'financial':
                # For financial data, you may want to include more fields
                writer.writerow(["date", "open", "high", "low", "close", "volume"])
                data_points = self.financial_data_points.all().order_by('date')
                for dp in data_points:
                    writer.writerow([dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume])
        
