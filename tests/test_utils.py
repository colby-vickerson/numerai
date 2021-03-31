import os

import pandas as pd
import pytest

import src.utils as utils


def mock_dataframe():
    """Mock a dataframe to save"""
    return pd.DataFrame(
        {
            "column_one": [1, 2, 3],
            "column_two": [2, 3, 4]
        }
    )


@pytest.mark.parametrize(
    "file_name, raises_error", [
        ("temp_file.parquet.gzip", False),
        ("temp_file.csv", False),
        ("temp_file.unknown", True),
        ("different_parquet.file", True),
        ("test.parquet", False)
    ]
)
def test_save_pandas_dataframe(temp_directory_path: str, file_name: str, raises_error: bool):
    """Test saving a pandas dataframe to disk"""
    absolute_output_path = os.path.join(
        temp_directory_path, file_name
    )
    if raises_error:
        with pytest.raises(LookupError):
            utils.save_pandas_dataframe(mock_dataframe(), absolute_output_path)
    else:
        utils.save_pandas_dataframe(mock_dataframe(), absolute_output_path)

        # Assert that the file was saved and now exists
        assert os.path.exists(absolute_output_path)


@pytest.mark.parametrize(
    "file_name, raises_error", [
        ("test_one.parquet.gzip", False),
        ("test_two.parquet", False),
        ("test_three.csv", False),
        ("wrong.file", True)
    ]
)
def test_load_data_as_pandas_dataframe(temp_directory_path: str, file_name: str, raises_error: bool):
    absolute_output_path = os.path.join(
        temp_directory_path, file_name
    )

    if raises_error:
        with pytest.raises(LookupError):
            utils.load_data_as_pandas(temp_directory_path)
    else:
        utils.save_pandas_dataframe(mock_dataframe(), absolute_output_path)

        # Read in the dataframe
        test_df = utils.load_data_as_pandas(absolute_output_path)

        # Assert data same as saved mock data
        pd.testing.assert_frame_equal(test_df, mock_dataframe())
