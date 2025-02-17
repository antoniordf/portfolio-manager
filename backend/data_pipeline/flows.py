from prefect import flow
from typing import List, Dict
from .tasks import fetch_and_store_multiple_series

@flow(name="Fetch and Store Multiple Series Flow")
def fetch_and_store_flow(data_series_list: List[Dict[str, str]]):
    """
    A simple Prefect flow that calls a single task (or tasks) with no concurrency.
    Everything runs sequentially in the main process.
    """
    # Just call the task in a flow context
    fetch_and_store_multiple_series(data_series_list)