"""Implements the post-download processing functions."""

import shutil
import pathlib
from typing import List, Union

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
