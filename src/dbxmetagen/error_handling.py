import time
import random
import pandas as pd

def exponential_backoff(retries, base_delay=1, max_delay=120, jitter=True):
    """
    Exponential backoff with optional jitter.

    :param retries: Number of retries attempted.
    :param base_delay: Initial delay in seconds.
    :param max_delay: Maximum delay in seconds.
    :param jitter: Whether to add random jitter to the delay.
    :return: None
    """
    delay = min(base_delay * (2 ** retries), max_delay)
    if jitter:
        delay = delay / 2 + random.uniform(0, delay / 2)
    time.sleep(delay)


def validate_csv(csv_path: str) -> bool:
    """
    Validates the CSV to ensure that a catalog is never given without a schema and a schema name never given without a table name.

    Args:
        csv_path (str): The path to the CSV file.

    Returns:
        bool: True if the CSV is valid, False otherwise.
    """
    csv_df = pd.read_csv(csv_path)

    for index, row in csv_df.iterrows():
        if row['catalog'] and not row['schema']:
            print(f"Invalid row at index {index}: Catalog is given without a schema.")
            return False
        if row['schema'] and not row['table']:
            print(f"Invalid row at index {index}: Schema is given without a table name.")
            return False

    return True
