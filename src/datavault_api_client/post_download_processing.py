"""Implements the post-download processing functions.

The post-download processing phase is differentiated by the type of the download that took
place (a synchronous or a concurrent download).

If a synchronous download took place, the post-download processing phase is as simple as
feeding to the function that tests the data integrity, the entire list of DownloadDetails
named-tuples containing the file-specific information of each file that has been
downloaded. The data integrity testing functions check if all the files have the expected
characteristics and, if any file fails the integrity test (meaning that it was not
completely or properly downloaded) it is included in a list of files whose download has
to be repeated.

If a concurrent download took place, at the end of the download process we will have a mix
of files and partitions that have been downloaded, depending on whether a file was
eligible to be partitioned given its size and the multi-part threshold. For the files that
have been downloaded as a whole, the post download processing is the same as in the case
of the synchronous download, they are just passed to the data integrity testing functions
and, if their characteristics differ from the expected ones, they are added to the list of
downloads to retry and the file download information are included in a list containing the
reference data of each file whose download has to be retried. In the case of partitioned
files, instead, the process is more involved. First, for each partitioned file,
the program checks the partitions corresponding to that file that have been downloaded;
if the list of found partitions match the expected list of partitions that was defined
pre-download, then the file is added to a list of files that are ready for the
concatenation phase. If, instead, any missing partition is detected, the missing
partitions are immediately added to a list of partitions to retry, and the corresponding
file information are added to the list containing the reference data of those files whose
download is to be repeated. All those files that have all the partitions downloaded are
then hand over to the concatenating functions that concatenate all the partitions in a
sequential order. The newly concatenated files are then passed to the integrity testing
functions and, if any file fails the integrity test, its partitions are added to the list
of files and partitions whose download is to be repeated.
"""

import pathlib
import shutil
from typing import Any, List, Tuple, Union
import itertools
from datavault_api_client.data_integrity import get_list_of_failed_downloads
from datavault_api_client.data_structures import (
    ConcurrentDownloadManifest,
    DownloadDetails,
    PartitionDownloadDetails,
)
from datavault_api_client.pre_download_processing import filter_files_to_split


# def post_download_processing(
#     downloaded_files: List[DownloadDetails],
#     synchronous: bool = False,
# ) -> List[DownloadDetails]:
#     if synchronous:
#         return get_list_of_failed_downloads(downloaded_files)


##########################################################################################


def get_non_partitioned_files(
    whole_files_reference_data: List[DownloadDetails],
) -> List[DownloadDetails]:
    return [file for file in whole_files_reference_data if file.is_partitioned is False]


def get_partitioned_files(
    whole_files_reference_data: List[DownloadDetails],
) -> List[DownloadDetails]:
    return list(
        set(whole_files_reference_data).difference(
            set(get_non_partitioned_files(whole_files_reference_data))
        )
    )


def get_partitions_download_details(
    concurrent_download_manifest: List[Union[DownloadDetails, PartitionDownloadDetails]],
    file_name: str = None
) -> List[PartitionDownloadDetails]:
    """Filters from the download manifest the PartitionDownloadDetails named-tuples.

    Parameters
    ----------
    concurrent_download_manifest: List[Union[DownloadDetails, PartitionDownloadDetails]]
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
            file for file in concurrent_download_manifest
            if type(file) is PartitionDownloadDetails
        ]
    return [
        file for file in concurrent_download_manifest
        if type(file) is PartitionDownloadDetails and file.parent_file_name == file_name
    ]


def get_downloaded_partitions(path_to_folder: pathlib.Path) -> List[Any]:
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
    downloaded_partitions = list(path_to_folder.glob("*.txt"))
    if len(downloaded_partitions) != 0:
        downloaded_partitions.sort(key=lambda x: int(x.stem.split("_")[3]))
    return downloaded_partitions


def get_file_specific_missing_partitions(
    file_specific_download_details: DownloadDetails,
    concurrent_download_manifest: List[Union[DownloadDetails, PartitionDownloadDetails]],
) -> List[Any]:
    """Compares the downloaded against the expected partitions and returns the missing partitions.

    Parameters
    ----------
    file_specific_download_details: DownloadDetails
        A DownloadDetails named-tuple containing file-specific download information.
    concurrent_download_manifest: List[Union[DownloadDetails, PartitionDownloadDetails]]
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
        concurrent_download_manifest,
        file_name=file_specific_download_details.file_name,
    )
    return [
        partition for partition in expected_partitions
        if partition.file_path not in get_downloaded_partitions(
            file_specific_download_details.file_path.parent,
        )
    ]


def get_all_missing_partitions(
    whole_files_reference_data: List[DownloadDetails],
    concurrent_download_manifest: List[Union[DownloadDetails, PartitionDownloadDetails]],
) -> List[PartitionDownloadDetails]:
    """Returns all the missing partitions from a download session.

    Parameters
    ----------
    whole_files_reference_data: List[DownloadDetails]
        A list of DownloadDetails named-tuples each containing file-specific download
        information.
    concurrent_download_manifest: List[Union[DownloadDetails, PartitionDownloadDetails]]
        A list of DownloadDetails and PartitionDownloadDetails named-tuples.

    Returns
    -------
    List[Any]
        If the function detects any missing partition, it will return a tuple containing a
        list of DownloadDetails named-tuple with the download information of all those files
        that are missing some partitions, and a list of PartitionDownloadDetails containing
        the download information of the missing partitions. If no missing partition is found,
        the function will return a tuple of empty lists.
    """
    missing_partitions = [
        get_file_specific_missing_partitions(file, concurrent_download_manifest)
        for file in get_partitioned_files(whole_files_reference_data)
        if len(get_file_specific_missing_partitions(file, concurrent_download_manifest)) > 0
    ]
    return list(itertools.chain.from_iterable(missing_partitions))


def get_files_with_missing_partitions(
    whole_files_reference_data: List[DownloadDetails],
    missing_partitions: List[PartitionDownloadDetails],
) -> List[DownloadDetails]:
    """Returns all the files with missing partitions.

    Parameters
    ----------
    whole_files_reference_data: List[DownloadDetails]
        A list of DownloadDetails named-tuples each containing file-specific download
        information.
    missing_partitions: List[PartitionDownloadDetails]
        A list of missing partition's PartitionDownloadDetails named-tuples.

    Returns
    -------
    List[DownloadDetails]
        If the function detects any missing partition, it will return a tuple containing a
        list of DownloadDetails named-tuple with the download information of all those files
        that are missing some partitions, and a list of PartitionDownloadDetails containing
        the download information of the missing partitions. If no missing partition is found,
        the function will return a tuple of empty lists.
    """
    unique_file_names = {partition.parent_file_name for partition in missing_partitions}
    return [file for file in whole_files_reference_data if file.file_name in unique_file_names]


def get_files_ready_for_concatenation(
    whole_files_reference_data: List[DownloadDetails],
    files_with_missing_partitions: List[DownloadDetails]
) -> List[DownloadDetails]:
    """Returns a list of files that are not missing any partition and are ready for concatenation.

    Parameters
    ----------
    whole_files_reference_data: List[DownloadDetails]
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
    return list(
        set(get_partitioned_files(whole_files_reference_data)).difference(
            files_with_missing_partitions,
        )
    )


def concatenate_partitions(path_to_output_file: pathlib.Path) -> str:
    """Concatenates .txt partition files into a single .txt.bz2 compressed file.

    Parameters
    ----------
    path_to_output_file: pathlib.Path
        A pathlib.Path object indicating where the file that is assembled out of the
        single partition files will be saved.

    Returns
    -------
    str
        The full path of the output file as a string.
    """
    available_partition_files = get_downloaded_partitions(path_to_output_file.parent)
    with path_to_output_file.open("wb") as outfile:
        for file_path in available_partition_files:
            with file_path.open("rb") as file_source:
                shutil.copyfileobj(file_source, outfile, length=(5 * 1024 * 1024))
            file_path.unlink()
    return path_to_output_file.as_posix()


def concatenate_each_file_partitions(
    files_to_concatenate: List[DownloadDetails],
) -> List[DownloadDetails]:
    """Concatenates the partition files of all the files with partitions to concatenate.

    Parameters
    ----------
    files_to_concatenate: List[DownloadDetails]
        A list of DownloadDetails named-tuples containing the download information of all
        those files that do not have any missing partition.
    """
    for file in files_to_concatenate:
        concatenate_partitions(file.file_path)
    return files_to_concatenate


def get_files_ready_for_integrity_test(
    files_with_missing_partitions: List[DownloadDetails],
    whole_files_reference_data: List[DownloadDetails],
) -> List[DownloadDetails]:
    """Returns a list of those files that are ready for the data integrity checks.

    The list of files ready for the data integrity checks is made of a combination of
    those files that were not split in multiple partitions, and those files that were
    split in multiple partitions but were not missing any partition after the download
    took place.

    Parameters
    ----------
    files_with_missing_partitions: List[DownloadDetails]
        A list of DownloadDetails named-tuples containing the information of those
        files that had missing partitions after the download was completed.
    whole_files_reference_data: List[DownloadDetails]
        A list of DownloadDetails named-tuples containing the information of all the
        files to download.

    Returns
    -------
    List[DownloadDetails]
        A list of DownloadDetails named-tuples containing the information of those files
        that are ready for the data integrity checks.
    """
    return list(set(whole_files_reference_data).difference(set(files_with_missing_partitions)))

##########################################################################################


def pre_concatenation_processing(download_manifest: ConcurrentDownloadManifest):
    missing_partitions = get_all_missing_partitions(
        whole_files_reference_data=download_manifest.whole_files_reference,
        concurrent_download_manifest=download_manifest.concurrent_download_manifest,
    )
    files_with_missing_partitions = get_files_with_missing_partitions(
        whole_files_reference_data=download_manifest.whole_files_reference,
        missing_partitions=missing_partitions,
    )
    return ConcurrentDownloadManifest(
        whole_files_reference=files_with_missing_partitions,
        concurrent_download_manifest=missing_partitions,
    )


def concatenation_processing(
    download_manifest: ConcurrentDownloadManifest,
    failed_downloads_manifest: ConcurrentDownloadManifest,
):
    files_ready_for_concatenation = get_files_ready_for_concatenation(
        whole_files_reference_data=download_manifest.whole_files_reference,
        files_with_missing_partitions=failed_downloads_manifest.whole_files_reference,
    )
    concatenate_each_file_partitions(files_ready_for_concatenation)


def update_failed_download_manifest(
    failed_download_manifest: ConcurrentDownloadManifest,
    failed_files: List[DownloadDetails],
    failed_partitions: List[PartitionDownloadDetails],
):
    pass
