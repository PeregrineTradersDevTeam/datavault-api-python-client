"""Implements the functions used to process the crawler data before the download takes place.

The functions in this module process the raw information contained in the list of
DiscoveredFileInfo named tuples that is produced by the crawler, and prepare the download
manifest that is used by the downloading functions as a reference.
"""
import pathlib
from typing import Dict, Iterable, List, Union
import urllib.parse

from datavault_api_client.data_structures import (
    DiscoveredFileInfo,
    DownloadDetails,
    PartitionDownloadDetails,
)


def generate_file_path_matching_datavault_structure(
    path_to_data_folder: str,
    file_name: str,
    datavault_download_url: str,
) -> pathlib.Path:
    """Generates a file path that follows the directory structure of the Datavault API.

    The files in the Datavault API are organised in a directory structure that respects
    the structure: <year>/<month>/<day>/<source>/<file-type>/<file-identifier>. The
    function extrapolates this structure from the download url of each file, and mounts
    the path on a user defined directory on the local file system.

    Parameters
    ----------
    path_to_data_folder: str
        The full path to the directory where the data will be downloaded according to
        the structure implied by the DataVault API.
    file_name: str
        The name of the file.
    datavault_download_url: str
        The download url of the file.

    Returns
    -------
    pathlib.Path
        A Path object that originates from the data folder specified by the user that
        respects the structure of the directory tree in the Datavault API.
    """
    datavault_path = urllib.parse.urlsplit(datavault_download_url).path
    relevant_path_components = datavault_path.split("/")[3:8]
    directory_path = pathlib.Path(path_to_data_folder).joinpath(
        "/".join(relevant_path_components),
    )
    return directory_path.joinpath(file_name)


def convert_mib_to_bytes(size_in_mib: float) -> int:
    """Converts a size expressed in MiB to Bytes.

    Parameters
    ----------
    size_in_mib: float
        A file size in MiB.

    Returns
    -------
    int
        The size in Bytes equivalent to the passed size in MiB.
    """
    return round(size_in_mib * (1024**2))
