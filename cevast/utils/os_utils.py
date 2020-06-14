"""
This module provide functions supporting work with OS.
"""
import os
import sys

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


def directory_with_prefix(directory: str, prefix: str, filename_only: bool = False) -> str:
    """Generator listing directory and returning paths with the specified prefix."""
    # Check if the directory exists
    if os.path.exists(directory):
        # Check if there is any file matching the prefix
        for file in os.listdir(directory):
            if file.startswith(prefix):
                if filename_only:
                    yield file
                else:
                    yield os.path.join(directory, file)


if __name__ == "__main__":
    try:
        remove_empty_folders(sys.argv[1])
    except IndexError:
        remove_empty_folders('.')
