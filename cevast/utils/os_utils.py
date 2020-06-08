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


if __name__ == "__main__":
    try:
        remove_empty_folders(sys.argv[1])
    except IndexError:
        remove_empty_folders('.')
