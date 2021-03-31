"""Test selecting features"""
import os

import mock
import numpy as np
import pandas as pd
import pytest

from src.processing.select_features import SelectFeatures


def mock_select_features_input():
    """Mock data for the select features test"""
    return pd.DataFrame(
        {
            "id_column": [1, 2, 3],
            "feature_one": [1, 2, 3],
            "backward_feature": [2, 3, 4],
            "feature_two": [3, 4, 5],
            "random_column": [5, 6, 7],
            "featureinname": [1, 2, 3]
        }
    )


@pytest.fixture(name="select_features_task", scope="module")
def instantiate_select_features_task(temp_directory_path):
    """Instantiate the DownloadData task"""
    select_feature_task = SelectFeatures(
        output_directory=temp_directory_path
    )
    with mock.patch(
            'src.download.download_data.DownloadData.load_output',
            mock.MagicMock(return_value=mock_select_features_input())
    ):
        yield select_feature_task


def test_select_features_task_feature_columns_selected(select_features_task):
    feature_df = select_features_task.run()
    np.testing.assert_array_equal(
        feature_df.columns,
        ["feature_one", "feature_two", "featureinname"]
    )


def test_output_exists(select_features_task):
    select_features_task.run()
    assert os.path.exists(select_features_task.output().path)
