

def determine_sampling_ratio(nrows: int, sample_size: int) -> float:
    """
    Takes a number of rows and a ratio, and returns the number of rows to sample.

    Args:
        nrows (int): The number of rows in the DataFrame.
        ratio (int): The ratio to use for sampling to avoid too many rows.

    Returns:
        ratio (float): The number of rows to sample.
    """
    if sample_size < nrows:
        ratio = sample_size / nrows
    else:
        ratio = 1.0
    print("Sampling ratio:", ratio)
    return ratio
