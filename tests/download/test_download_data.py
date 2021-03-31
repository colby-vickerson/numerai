"""Contains tests for the download_data module"""
import os

import luigi
import pytest

from src.download.download_data import DownloadTrainingData


@pytest.fixture(name="download_data_task", scope="module")
def instantiate_download_data_task(temp_directory_path):
    """Instantiate the DownloadData task"""
    yield DownloadTrainingData(
        output_directory=temp_directory_path
    )


@pytest.fixture(name="executed_download_data_task", scope="module")
def run_download_data_task(download_data_task):
    """Run the DownloadData task and yield the run task"""
    luigi.build(
        [
            download_data_task
        ],
        local_scheduler=True
    )
    yield download_data_task


def test_no_path_before_task_runs(download_data_task):
    """Test that no path exists prior to the task being run"""
    assert not os.path.exists(
        download_data_task.output().path
    )


def test_output_created(executed_download_data_task):
    """Test that the output file exists after the task is run"""
    assert os.path.exists(
        executed_download_data_task.output().path
    )


def test_zip_file_deleted(executed_download_data_task):
    """Verify that the zip file is deleted"""
    assert not os.path.exists(
        os.path.join(
            executed_download_data_task.output_directory,
            executed_download_data_task.get_zipped_current_tournament_file_name()
        )
    )
