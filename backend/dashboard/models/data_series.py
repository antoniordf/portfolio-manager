from django.db import models
from data_pipeline.utils.bigquery_utils import (
    fetch_time_series,
    insert_economic_row,
    insert_financial_row,
    merge_data_point,
    count_observations,
    data_point_exists,
    get_latest_data_point
)

# Parent class: Represents the metadata and methods for the time series
class DataSeries(models.Model):
    name = models.CharField(max_length=150)
    series_id = models.CharField(max_length=150, unique=True)
    observation_start = models.DateField(null=True, blank=True)
    observation_end = models.DateField(null=True, blank=True)
    frequency = models.CharField(max_length=150)
    units = models.CharField(max_length=150)
    seasonal_adjustment = models.CharField(max_length=150)
    last_updated = models.DateTimeField(auto_now=True)
    notes = models.TextField(blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    last_data_date = models.DateField(null=True, blank=True, help_text="Date of the most recent data point in the time series.")

    DATA_ORIGINS = (
        ('fred', 'FRED'),
        ('polygon', 'Polygon.io'),
    )

    data_origin = models.CharField(max_length=20, choices=DATA_ORIGINS)
    
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
    
    def get_time_series(self, start_date=None, end_date=None) -> list:
        """
        Return time series data from BigQuery. 
        For 'economic': list of (date, value).
        For 'financial': list of (date, open, high, low, close, volume).
        """
        return fetch_time_series(
            data_type=self.data_type,
            series_id=self.series_id,
            start_date=start_date,
            end_date=end_date
        )

    def add_data_point(self, date, **kwargs) -> None:
        """
        Insert a single row. 
        For 'economic': requires 'value'.
        For 'financial': requires 'open', 'high', 'low', 'close', 'volume'.
        """
        if self.data_type == 'economic':
            if 'value' not in kwargs:
                raise ValueError("Missing 'value' for economic data.")
            insert_economic_row(self.series_id, str(date), kwargs['value'])
        elif self.data_type == 'financial':
            required = ['open', 'high', 'low', 'close', 'volume']
            for field in required:
                if field not in kwargs:
                    raise ValueError(f"Missing '{field}' for financial data.")
            insert_financial_row(
                self.series_id, str(date),
                kwargs['open'],
                kwargs['high'],
                kwargs['low'],
                kwargs['close'],
                kwargs['volume']
            )
        else:
            raise ValueError(f"Unknown data_type '{self.data_type}' in add_data_point.")
    
    def add_data_point(self, date, **kwargs) -> None:
        """
        Insert new row to BigQuery by calling bigquery_utils insert methods
        to avoid repeating table references here.
        """
        if self.data_type == 'economic':
            if 'value' not in kwargs:
                raise ValueError("Missing 'value' for economic data.")
            # Use the utility function to insert economic data
            insert_economic_row(self.series_id, str(date), kwargs['value'])
        else:
            required = ['open', 'high', 'low', 'close', 'volume']
            for f in required:
                if f not in kwargs:
                    raise ValueError(f"Missing '{f}' for financial data.")
            insert_financial_row(
                self.series_id,
                str(date),
                kwargs['open'],
                kwargs['high'],
                kwargs['low'],
                kwargs['close'],
                kwargs['volume'],
            )
    
    def update_data_point(self, date, **kwargs) -> None:
        """
        Merge logic. 
        For 'economic': pass 'value'.
        For 'financial': pass any of ['open','high','low','close','volume'].
        """
        merge_data_point(
            data_type=self.data_type,
            series_id=self.series_id,
            date_str=str(date),
            **kwargs
        )
            
    def count_observations(self) -> int:
        """
        Return row count for series_id in the relevant table.
        """
        return count_observations(self.data_type, self.series_id)

    def data_point_exists(self, date) -> bool:
        """
        Check if row (series_id, date) exists.
        """
        return data_point_exists(self.data_type, self.series_id, str(date))

    def get_latest_data_point(self):
        """
        Return the most recent row as a dict, or None if no rows exist.
        """
        return get_latest_data_point(self.data_type, self.series_id)