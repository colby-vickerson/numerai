import tempfile

import pytest


@pytest.fixture(name="temp_directory_path", scope="module")
def create_temp_directory():
    """Set up directory for running tests"""
    with tempfile.TemporaryDirectory() as directory:
        yield directory
