"""Contains tests for the download_data module"""
import pytest
from src.download.download_data import DownloadData
import tempfile


@pytest.fixture
def download_data_setup():
    """Set up directory for running tests"""
    with tempfile.TemporaryDirectory() as directory:
        yield directory


def test_download_data(download_data_setup):
    pass