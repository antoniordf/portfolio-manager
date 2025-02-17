import datetime
import pandas as pd
import graphene
from django.core.cache import cache
import datetime
from dashboard.models import DataSeries
from .types import FinancialDataPointType 
from .types import QuadrantDataPointType
from dashboard.models import DataSeries
from dashboard.utils.returns import calculate_returns

class Query(graphene.ObjectType):
    quadrant_data = graphene.List(
        QuadrantDataPointType,
        start_date=graphene.Date(),
        end_date=graphene.Date(),
        data_points=graphene.Int(default_value=15)
    )

    stock_time_series = graphene.List(
        FinancialDataPointType,
        series_id=graphene.String(required=True),
        start_date=graphene.Date(),
        end_date=graphene.Date(),
    )

    def resolve_quadrant_data(self, info, start_date=None, end_date=None, data_points=15):
        """
        GraphQL resolver for quadrant data (GDP vs. Inflation acceleration).
        Uses BigQuery-backed DataSeries objects and calculates YoY returns 
        plus rate_of_change. Applies caching with a key based on the 
        last_updated timestamps and input parameters.
        """

        # 1) Attempt to find the relevant DataSeries
        try:
            real_gdp_series = DataSeries.objects.get(series_id='GDPC1')      # e.g., freq="Quarterly"
            inflation_series = DataSeries.objects.get(series_id='CPIAUCSL')  # e.g., freq="Monthly"
        except DataSeries.DoesNotExist:
            return []

        # 2) Build a cache key
        last_updated_gdp = real_gdp_series.last_updated
        last_updated_infl = inflation_series.last_updated
        last_updated = max(last_updated_gdp, last_updated_infl)
        last_updated_timestamp = int(last_updated.timestamp())

        cache_key = f"quadrant_data_{start_date}_{end_date}_{data_points}_{last_updated_timestamp}"
        cached_data = cache.get(cache_key)
        if cached_data:
            # Deserialize
            return [
                QuadrantDataPointType(
                    date=datetime.datetime.strptime(item["date"], "%Y-%m-%d").date(),
                    gdp_growth=item["gdp_growth"],
                    inflation_growth=item["inflation_growth"],
                )
                for item in cached_data
            ]

        # 3) Fetch raw data from BigQuery via DataSeries
        #    Each returns a list of (date, value) because data_type='economic'.
        gdp_list = real_gdp_series.get_time_series()
        infl_list = inflation_series.get_time_series()

        # 4) Calculate YoY changes
        #    For GDP, pass data_frequency="Quarterly"
        #    For Inflation, pass data_frequency="Monthly"
        gdp_df = calculate_returns(gdp_list, data_frequency="Quarterly")
        infl_df = calculate_returns(infl_list, data_frequency="Monthly")

        # 5) Resample inflation to quarterly start
        infl_q = infl_df.resample("QS").first()

        # 6) Align the two DataFrames on the same index, drop NaNs
        infl_aligned = infl_q.reindex(gdp_df.index).dropna()
        gdp_df = gdp_df.loc[infl_aligned.index]

        # 7) Compute rate_of_change (acceleration) for both GDP & Inflation
        gdp_df["rate_of_change"] = gdp_df["yoy_change"] - gdp_df["yoy_change"].shift(1)
        infl_aligned["rate_of_change"] = (
            infl_aligned["yoy_change"] - infl_aligned["yoy_change"].shift(1)
        )
        gdp_df.dropna(inplace=True)
        infl_aligned.dropna(inplace=True)

        common_dates = gdp_df.index.intersection(infl_aligned.index)
        gdp_df = gdp_df.loc[common_dates]
        infl_aligned = infl_aligned.loc[common_dates]

        # 8) Combine them
        combined = pd.concat([gdp_df, infl_aligned], axis=1, keys=["gdp", "inflation"])
        combined.columns = ["_".join(col) for col in combined.columns]

        # 9) Filter by start_date / end_date
        if start_date:
            combined = combined[combined.index >= pd.to_datetime(start_date)]
        if end_date:
            combined = combined[combined.index <= pd.to_datetime(end_date)]

        # 10) Take the last N data points
        combined = combined.tail(data_points)

        # 11) Build final result rows
        results = []
        for dt_idx, row in combined.iterrows():
            results.append({
                "date": dt_idx.date(),
                "gdp_growth": round(row["gdp_rate_of_change"] * 100, 4),
                "inflation_growth": round(row["inflation_rate_of_change"] * 100, 4),
            })

        # 12) Cache the serialized data
        to_cache = []
        for r in results:
            to_cache.append({
                "date": r["date"].isoformat(),
                "gdp_growth": r["gdp_growth"],
                "inflation_growth": r["inflation_growth"],
            })
        cache.set(cache_key, to_cache, timeout=None)

        # 13) Return as QuadrantDataPointType
        return [
            QuadrantDataPointType(
                date=r["date"],
                gdp_growth=r["gdp_growth"],
                inflation_growth=r["inflation_growth"]
            )
            for r in results
        ]
    
    def resolve_stock_time_series(self, info, series_id, start_date=None, end_date=None):
        """
        Refactored to query BigQuery via DataSeries.get_time_series() instead of
        local FinancialDataPoint objects. Preserves caching logic.
        """
        # 1) Get the DataSeries object
        try:
            series = DataSeries.objects.get(series_id=series_id, data_type='financial')
        except DataSeries.DoesNotExist:
            return []

        # 2) Compute last_updated-based cache key
        last_updated_timestamp = int(series.last_updated.timestamp())
        cache_key = f'stock_time_series_{series_id}_{start_date}_{end_date}_{last_updated_timestamp}'

        # 3) Check cache
        cached_data = cache.get(cache_key)
        if cached_data:
            return [
                FinancialDataPointType(
                    date=datetime.datetime.strptime(item['date'], '%Y-%m-%d').date(),
                    open=item['open'],
                    high=item['high'],
                    low=item['low'],
                    close=item['close'],
                    volume=item['volume'],
                )
                for item in cached_data
            ]

        # 4) Fetch time series data from BigQuery
        #    get_time_series(...) for financial returns: [(date, open, high, low, close, volume), ...]
        raw_data = series.get_time_series(start_date, end_date)

        # 5) Serialize for caching
        #    row[0] = date, row[1] = open, row[2] = high, row[3] = low, row[4] = close, row[5] = volume
        serialized_data = []
        for row in raw_data:
            serialized_data.append({
                'date': row[0].isoformat() if row[0] else None,
                'open': row[1],
                'high': row[2],
                'low': row[3],
                'close': row[4],
                'volume': row[5],
            })

        # 6) Cache
        cache.set(cache_key, serialized_data, timeout=None)

        # 7) Return as GraphQL FinancialDataPointType
        return [
            FinancialDataPointType(
                date=row[0],
                open=row[1],
                high=row[2],
                low=row[3],
                close=row[4],
                volume=row[5],
            )
            for row in raw_data
        ]