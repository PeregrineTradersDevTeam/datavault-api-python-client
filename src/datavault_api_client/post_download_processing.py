"""Implements the post-download processing functions."""

import shutil
import pathlib
from typing import Any, List, Union

from datavault_api_client.data_integrity import get_list_of_failed_downloads
from datavault_api_client.data_structures import DownloadDetails, PartitionDownloadDetails
from datavault_api_client.pre_download_processing import filter_files_to_split


def get_partitions_download_details(
    files_and_partitions_download_manifest: List[Union[DownloadDetails, PartitionDownloadDetails]],
    file_name: str = None
) -> List[PartitionDownloadDetails]:
    """Filters from the download manifest the PartitionDownloadDetails named-tuples.

    Parameters
    ----------
    files_and_partitions_download_manifest: List[Union[DownloadDetails, PartitionDownloadDetails]]
        A list of DownloadDetails and PartitionDownloadDetails named-tuples, constituting
        the  download manifest of all the whole files and file partitions to download.
    file_name: str
        The name of a file. By default is set to None.

    Returns
    -------
    List[PartitionDownloadDetails]
        A list of PartitionDownloadDetails named-tuples. If no file_name is passed, the
        function will return all the PartitionDownloadDetails named-tuples that are found
        in the download manifest. If, instead, a file name is passed, the function will
        return only the PartitionDownloadDetails named-tuples that belong to that specific
        file.
    """
    if not file_name:
        return [
            file for file in files_and_partitions_download_manifest
            if type(file) is PartitionDownloadDetails
        ]
    return [
        file for file in files_and_partitions_download_manifest
        if type(file) is PartitionDownloadDetails and file.parent_file_name == file_name
    ]


def get_partition_index(path_to_partition: pathlib.Path) -> int:
    """Retrieves the partition index from a partition file name.

    Parameters
    ----------
    path_to_partition: pathlib.Path
        A pathlib.Path object containing the full path to the partition file.

    Returns
    -------
    int
        The partition index as an integer.

    Notes
    -----
    Each partition file is named according to the format:
    <FILE-TYPE>_<SOURCE-ID>_<DATE>_<PARTITION_INDEX>.txt
    This standardised structure is used by the function to consistently retrieve the
    index of a partition.
    """
    return int(path_to_partition.stem.split("_")[3])


def get_list_of_downloaded_partitions(path_to_folder: pathlib.Path) -> List[Any]:
    """Retrieves the full paths of the partitions file in a folder.

    Parameters
    ----------
    path_to_folder: pathlib.Path
        A pathlib.Path indicating the full path to the directory where we want to check
        for partition files.

    Returns
    -------
    List[Any]
        If in the directory that is passed as an input are found partition files, the
        function will return a list of pathlib.Path objects each containing the full
        path to an individual partition file. If no partition file is found in the
        directory, the function will return an empty list.
    """
    list_of_partition_files_in_folder = list(path_to_folder.glob("*.txt"))
    if len(list_of_partition_files_in_folder) != 0:
        list_of_partition_files_in_folder.sort(key=get_partition_index)
    return list_of_partition_files_in_folder

