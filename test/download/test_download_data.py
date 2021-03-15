"""Contains tests for the download_data module"""
import os
import tempfile

import luigi
import pytest

from src.download.download_data import DownloadData


@pytest.fixture(name="temp_directory_path")
def create_temp_directory():
    """Set up directory for running tests"""
    with tempfile.TemporaryDirectory() as directory:
        yield directory


def test_download_data(temp_directory_path):
    download_data_task = DownloadData(
        output_directory=temp_directory_path
    )

    # Assert that the path does not exist prior to the task run
    assert not os.path.exists(
        download_data_task.output().path
    )

    # Run the task
    luigi.build(
        [
            download_data_task
        ],
        local_scheduler=True
    )

    # Verify that the output exists
    assert os.path.exists(
        download_data_task.output().path
    )
