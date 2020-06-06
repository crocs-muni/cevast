"""
This module provide functions supporting work with OS.
"""
import os

__author__ = 'Radim Podola'


def remove_empty_folders(path: str):
    """Recursively remove empty folders"""
    files = os.listdir(path)
    if files:
        # Remove empty subfolders
        for file in files:
            fullpath = os.path.join(path, file)
            if os.path.isdir(fullpath):
                remove_empty_folders(fullpath)
    else:
        os.rmdir(path)
