"""Test selecting features"""
import pytest
import pandas as pd
from src.processing.select_features import SelectFeatures


@pytest.fixture(name="select_features_task", scope="module")
def instantiate_select_features_task(temp_directory_path):
    """Instantiate the DownloadData task"""
    select_feature_task = SelectFeatures(
        output_directory=temp_directory_path
    )

    yield select_feature_task


def get_required_tasks(given_task):
    """Get the required tasks for a given task"""
    return given_task.requires()


def save_data_to_task_output(task, data):
    """Save data to the tasks output location for testing purposes"""
    pass


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

