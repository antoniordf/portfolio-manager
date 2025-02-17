import pandas as pd
import logging

logger = logging.getLogger(__name__)

def calculate_returns(time_series, data_frequency):
    """
    Given a list of (date, value) pairs and a data_frequency ("Monthly", "Quarterly"),
    return a pandas DataFrame with columns ['value', 'yoy_change'].

    :param time_series: list of tuples/list: [(date, value), (date, value), ...]
    :param data_frequency: str, e.g. "Monthly" or "Quarterly"
    :return: pd.DataFrame with columns 'value' and 'yoy_change'
    """
    if not time_series:
        logger.warning("Empty time_series passed to calculate_returns.")
        # Return empty DataFrame
        return pd.DataFrame(columns=['value', 'yoy_change'])

    # Convert list of tuples into a DataFrame
    df = pd.DataFrame(time_series, columns=['date', 'value'])

    # Convert 'date' to datetime, set as index, sort
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

    result_df = pd.DataFrame({
        'value': df['value'],
        'yoy_change': yoy_returns,
    })

    # Drop rows with NaN from yoy_change
    result_df.dropna(inplace=True)
    return result_df
