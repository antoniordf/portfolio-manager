import logging
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from django.conf import settings

# Constants for your table IDs
ECONOMIC_TABLE_ID = settings.ECONOMIC_TABLE_ID
FINANCIAL_TABLE_ID = settings.FINANCIAL_TABLE_ID

# A single shared BigQuery client
client = bigquery.Client()


def _build_partition_filter_clause(data_type: str, start_date=None, end_date=None) -> str:
    """
    Return a string that adds a partition filter for day or month based on data_type,
    or return "" if no filter is needed.
    """
    if not start_date or not end_date:
        return ""
    
    return "AND date BETWEEN @start_date AND @end_date"


def fetch_time_series(
    data_type: str,
    series_id: str,
    start_date=None,
    end_date=None
) -> list:
    """
    Fetch rows for a given series_id (and optional date range).
    Returns a list of tuples, either:
      - economic: (date, value)
      - financial: (date, open, high, low, close, volume)
    """
    if data_type == 'economic':
        table_id = ECONOMIC_TABLE_ID
        select_cols = "date, value"
    elif data_type == 'financial':
        table_id = FINANCIAL_TABLE_ID
        select_cols = "date, open, high, low, close, volume"
    else:
        raise ValueError(f"Unknown data_type {data_type}")

    partition_clause = _build_partition_filter_clause(data_type, start_date, end_date)

    query_str = f"""
        SELECT {select_cols}
        FROM `{table_id}`
        WHERE series_id = @series_id
        {partition_clause}
        ORDER BY date
    """

    query_params = [bigquery.ScalarQueryParameter("series_id", "STRING", series_id)]
    if start_date and end_date:
        query_params.append(bigquery.ScalarQueryParameter("start_date", "DATE", str(start_date)))
        query_params.append(bigquery.ScalarQueryParameter("end_date", "DATE", str(end_date)))

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    try:
        query_job = client.query(query_str, job_config=job_config)
        results = query_job.result()
    except GoogleAPIError:
        logging.exception("Error in fetch_time_series")
        raise

    data = []
    if data_type == 'economic':
        for row in results:
            date_val = row.date
            val = float(row.value) if row.value is not None else None
            data.append((date_val, val))
    else:
        for row in results:
            date_val = row.date
            open_ = float(row.open) if row.open is not None else None
            high_ = float(row.high) if row.high is not None else None
            low_ = float(row.low) if row.low is not None else None
            close_ = float(row.close) if row.close is not None else None
            vol_ = int(row.volume) if row.volume is not None else None
            data.append((date_val, open_, high_, low_, close_, vol_))

    return data


def insert_economic_row(series_id: str, date_str: str, value: float):
    row_to_insert = {
        "series_id": series_id,
        "date": date_str,
        "value": value,
    }
    try:
        errors = client.insert_rows_json(ECONOMIC_TABLE_ID, [row_to_insert])
        if errors:
            raise ValueError(f"BigQuery insert errors: {errors}")
    except GoogleAPIError:
        logging.exception("Error in insert_economic_row")
        raise


def insert_financial_row(series_id: str, date_str: str, open_p, high_p, low_p, close_p, volume):
    row_to_insert = {
        "series_id": series_id,
        "date": date_str,
        "open": open_p,
        "high": high_p,
        "low": low_p,
        "close": close_p,
        "volume": volume,
    }
    try:
        errors = client.insert_rows_json(FINANCIAL_TABLE_ID, [row_to_insert])
        if errors:
            raise ValueError(f"BigQuery insert errors: {errors}")
    except GoogleAPIError:
        logging.exception("Error in insert_financial_row")
        raise


def merge_data_point(
    data_type: str,
    series_id: str,
    date_str: str,
    **kwargs
) -> None:
    """
    MERGE logic for economic or financial rows.
    Pass value=... for economic,
    or open=..., high=..., low=..., close=..., volume=... for financial.
    """
    if data_type == 'economic':
        table_id = ECONOMIC_TABLE_ID
        required_field = 'value'
        if required_field not in kwargs:
            raise ValueError(f"Missing '{required_field}' for economic MERGE.")
        set_clause = "T.value = @value"
        insert_cols = "(series_id, date, value)"
        insert_vals = "(@series_id, @date, @value)"
    elif data_type == 'financial':
        table_id = FINANCIAL_TABLE_ID
        allowed = ['open', 'high', 'low', 'close', 'volume']
        set_list = []
        for col in allowed:
            if col in kwargs:
                set_list.append(f"T.{col} = @{col}")
        if not set_list:
            raise ValueError("No fields to update or insert for financial MERGE.")
        set_clause = ", ".join(set_list)
        insert_cols = "(series_id, date, open, high, low, close, volume)"
        insert_vals = "(@series_id, @date, @open, @high, @low, @close, @volume)"
    else:
        raise ValueError(f"Unknown data_type {data_type}")

    merge_sql = f"""
        MERGE `{table_id}` T
        USING (
          SELECT @series_id AS series_id, @date AS date
        ) S
        ON T.series_id = S.series_id AND T.date = S.date
        WHEN MATCHED THEN
          UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
          INSERT {insert_cols}
          VALUES {insert_vals};
    """

    params = [
        bigquery.ScalarQueryParameter("series_id", "STRING", series_id),
        bigquery.ScalarQueryParameter("date", "STRING", date_str),
    ]
    if data_type == 'economic':
        params.append(bigquery.ScalarQueryParameter("value", "FLOAT64", kwargs['value']))
    else:
        # financial
        allowed_fin = {'open': None, 'high': None, 'low': None, 'close': None, 'volume': None}
        for key in allowed_fin:
            param_type = "INT64" if key == 'volume' else "FLOAT64"
            params.append(bigquery.ScalarQueryParameter(key, param_type, kwargs.get(key, None)))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    try:
        query_job = client.query(merge_sql, job_config=job_config)
        query_job.result()
    except GoogleAPIError:
        logging.exception("Error in merge_data_point")
        raise


def count_observations(data_type: str, series_id: str) -> int:
    """
    Return count of rows for the given series_id in the appropriate table.
    """
    table_id = ECONOMIC_TABLE_ID if data_type == 'economic' else FINANCIAL_TABLE_ID
    sql = f"""
        SELECT COUNT(*) as cnt
        FROM `{table_id}`
        WHERE series_id = @series_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("series_id", "STRING", series_id)]
    )
    try:
        query_job = client.query(sql, job_config=job_config)
        df = query_job.result().to_dataframe()
        return int(df["cnt"].iloc[0])
    except GoogleAPIError:
        logging.exception("Error in count_observations")
        raise


def data_point_exists(data_type: str, series_id: str, date_str: str) -> bool:
    """
    Check if row (series_id, date) exists in the relevant table.
    """
    table_id = ECONOMIC_TABLE_ID if data_type == 'economic' else FINANCIAL_TABLE_ID
    sql = f"""
        SELECT COUNT(*) as cnt
        FROM `{table_id}`
        WHERE series_id = @series_id
          AND date = @date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("series_id", "STRING", series_id),
            bigquery.ScalarQueryParameter("date", "STRING", date_str),
        ]
    )
    try:
        query_job = client.query(sql, job_config=job_config)
        row = next(iter(query_job.result()), None)
        return (row.cnt > 0) if row else False
    except GoogleAPIError:
        logging.exception("Error in data_point_exists")
        raise


def get_latest_data_point(data_type: str, series_id: str):
    """
    Return the most recent row for a series_id, or None if no rows exist.
    """
    if data_type == 'economic':
        table_id = ECONOMIC_TABLE_ID
        select_cols = "date, value"
    elif data_type == 'financial':
        table_id = FINANCIAL_TABLE_ID
        select_cols = "date, open, high, low, close, volume"
    else:
        raise ValueError(f"Unknown data_type {data_type}")

    sql = f"""
        SELECT {select_cols}
        FROM `{table_id}`
        WHERE series_id = @series_id
        ORDER BY date DESC
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("series_id", "STRING", series_id)]
    )
    try:
        query_job = client.query(sql, job_config=job_config)
        rows = list(query_job.result())
        if not rows:
            return None
        row = rows[0]
    except GoogleAPIError:
        logging.exception("Error in get_latest_data_point")
        raise

    if data_type == 'economic':
        return {
            "date": row.date,
            "value": float(row.value) if row.value is not None else None
        }
    else:
        return {
            "date": row.date,
            "open":   float(row.open)   if row.open   is not None else None,
            "high":   float(row.high)   if row.high   is not None else None,
            "low":    float(row.low)    if row.low    is not None else None,
            "close":  float(row.close)  if row.close  is not None else None,
            "volume": int(row.volume)   if row.volume is not None else None,
        }