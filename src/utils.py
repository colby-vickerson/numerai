import pandas as pd


def save_pandas_dataframe(dataframe: pd.DataFrame, absolute_output_location: str):
    """Save a pandas dataframe to disk"""
    if ".parquet" in absolute_output_location:
        dataframe.to_parquet(absolute_output_location)
    elif absolute_output_location.endswith(".csv"):
        dataframe.to_csv(absolute_output_location, index=False)
    else:
        raise LookupError("Unable to determine file type. Only support saving .csv and .parquet files.")
    return 1


def load_data_as_pandas(absolute_file_path):
    """Load in a file as a pandas dataframe"""
    if ".parquet" in absolute_file_path:
        return pd.read_parquet(absolute_file_path)
    elif absolute_file_path.endswith(".csv"):
        return pd.read_csv(absolute_file_path)
    else:
        raise LookupError("Unable to determine file type. Only support reading .csv and .parquet files.")
