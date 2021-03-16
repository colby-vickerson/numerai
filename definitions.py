"""Contains information on file/directory names/paths"""
import os

ROOT_DIR = os.path.dirname(os.path.realpath(__file__))

# Default Directories
DATA_DIR = os.path.join(
    ROOT_DIR,
    "data"
)
RAW_DATA_DIR = os.path.join(
    DATA_DIR,
    "raw"
)
