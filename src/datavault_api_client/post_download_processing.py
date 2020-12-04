"""Implements the post-download processing functions."""

import shutil
import pathlib
from typing import Any, List, Union, Tuple

from datavault_api_client.data_integrity import get_list_of_failed_downloads
from datavault_api_client.data_structures import DownloadDetails, PartitionDownloadDetails
from datavault_api_client.pre_download_processing import filter_files_to_split


def get_partitions_download_details(
    files_and_partitions_download_manifest: List[Union[DownloadDetails, PartitionDownloadDetails]],
    file_name: str = None,
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


def get_list_of_missing_partitions(
    file_specific_download_details: DownloadDetails,
    files_and_partitions_download_manifest: List[Union[DownloadDetails, PartitionDownloadDetails]],
) -> List[Any]:
    """Compares the downloaded against the expected partitions and returns the missing partitions.

    Parameters
    ----------
    file_specific_download_details: DownloadDetails
        A DownloadDetails named-tuple containing file-specific download information.
    files_and_partitions_download_manifest: List[Union[DownloadDetails, PartitionDownloadDetails]]
        A list of DownloadDetails and PartitionDownloadDetails named-tuples.

    Returns
    -------
    List[Any]
        If any missing partition is detected, the function will return a list of
        PartitionDownloadDetails named-tuples, one for each missing partition. If,
        instead, no missing partition is found, the function will return an empty
        list.
    """
    expected_partitions = get_partitions_download_details(
        files_and_partitions_download_manifest,
        file_name=file_specific_download_details.file_name,
    )

    expected_partitions_paths = {
        partition.file_path for partition in expected_partitions
    }
    downloaded_partitions_paths = set(
        get_list_of_downloaded_partitions(
            file_specific_download_details.file_path.parent,
        )
    )

    missing_partitions_paths = expected_partitions_paths.difference(
        downloaded_partitions_paths
    )

    return [
        partition for partition in expected_partitions
        if partition.file_path in missing_partitions_paths
    ]


def get_all_missing_partitions_and_corresponding_file_references(
    whole_files_download_manifest: List[DownloadDetails],
    files_and_partitions_download_manifest: List[Union[DownloadDetails, PartitionDownloadDetails]],
) -> Tuple[List[Any], List[Any]]:
    """Checks for missing partitions and return a list of them and their corresponding files.

    Parameters
    ----------
    whole_files_download_manifest: List[DownloadDetails]
        A list of DownloadDetails named-tuples each containing file-specific download
        information.
    files_and_partitions_download_manifest: List[Union[DownloadDetails, PartitionDownloadDetails]]
        A list of DownloadDetails and PartitionDownloadDetails named-tuples.

    Returns
    -------
    Tuple[List[Any], List[Any]]
    If the function detects any missing partition, it will return a tuple containing a
    list of DownloadDetails named-tuple with the download information of all those files
    that are missing some partitions, and a list of PartitionDownloadDetails containing
    the download information of the missing partitions. If no missing partition is found,
    the function will return a tuple of empty lists.
    """
    list_of_all_partitions_download_details = get_partitions_download_details(
        files_and_partitions_download_manifest,
    )
    all_missing_partitions = []
    files_with_missing_partitions = []
    for partitioned_file in filter_files_to_split(whole_files_download_manifest):
        missing_partitions = get_list_of_missing_partitions(
            partitioned_file, list_of_all_partitions_download_details,
        )
        if len(missing_partitions) > 0:
            files_with_missing_partitions.append(partitioned_file)
            all_missing_partitions += missing_partitions
    return files_with_missing_partitions, all_missing_partitions


def filter_files_ready_for_concatenation(
    whole_files_download_manifest: List[DownloadDetails],
    files_with_missing_partitions: List[DownloadDetails],
) -> List[DownloadDetails]:
    """Filters those files that are not missing any partition and thus are ready for concatenation.

    Parameters
    ----------
    whole_files_download_manifest: List[DownloadDetails]
        A list of DownloadDetails named-tuples each containing file-specific download
        information.
    files_with_missing_partitions: List[DownloadDetails]
        A list of DownloadDetails named-tuples each containing file-specific download
        information for all those files that are missing some partitions after the
        download.

    Returns
    -------
    List[DownloadDetails]
        A list of DownloadDetails named-tuples each containing file-specific download
        information for all those files that are not missing any partition after the
        download and that therefore are ready to have their partitions concatenated in
        a single file.
    """
    partitioned_files = set(filter_files_to_split(whole_files_download_manifest))
    files_missing_partitions = set(files_with_missing_partitions)
    files_ready_for_concatenation = list(
        partitioned_files.difference(files_missing_partitions),
    )
    if len(files_ready_for_concatenation) != 0:
        files_ready_for_concatenation.sort(key=lambda x: x.file_name)
    return files_ready_for_concatenation
