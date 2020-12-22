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
import copy
import itertools
import pathlib
import shutil
from typing import Any, List, Optional

from datavault_api_client.data_integrity import get_list_of_failed_downloads
from datavault_api_client.data_structures import (
    ConcurrentDownloadManifest,
    DownloadDetails,
    PartitionDownloadDetails,
)


##########################################################################################


def get_non_partitioned_files(
    whole_files_reference_data: List[DownloadDetails],
) -> List[DownloadDetails]:
    """Returns a list of non-partitioned files.

    Whether a file is partitioned or not is assessed by checking the is_partitioned flag
    in the DownloadDetails named-tuple.

    Parameters
    ----------
    whole_files_reference_data: List[DownloadDetails]
        A list of DownloadDetails named tuples containing the information of each downloaded
        file.

    Returns
    -------
    List[DownloadDetails]
        A list of DownloadDetails named-tuples of all those files where the is_partitioned
        flag is False.
    """
    return [file for file in whole_files_reference_data if file.is_partitioned is False]


def get_partitioned_files(
    whole_files_reference_data: List[DownloadDetails],
) -> List[DownloadDetails]:
    """Returns a list of partitioned files.

    Parameters
    ----------
    whole_files_reference_data: List[DownloadDetails]
        A list of DownloadDetails named tuples containing the information of each downloaded
        file.

    Returns
    -------
    List[DownloadDetails]
        A list of DownloadDetails named-tuples of all those files where the is_partitioned
        flag is True.
    """
    return [file for file in whole_files_reference_data if file.is_partitioned is True]


def get_partitions_download_details(
    partitions_to_download: List[PartitionDownloadDetails],
    file_name: Optional[str] = None,
) -> List[PartitionDownloadDetails]:
    """Filters from the download manifest the PartitionDownloadDetails named-tuples.

    Parameters
    ----------
    partitions_to_download: List[PartitionDownloadDetails]
        A list of PartitionDownloadDetails named-tuples.
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
        return partitions_to_download
    return [
        file for file in partitions_to_download
        if file.parent_file_name == file_name
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
    partitions_to_download: List[PartitionDownloadDetails],
) -> List[Any]:
    """Compares the downloaded against the expected partitions and returns the missing partitions.

    Parameters
    ----------
    file_specific_download_details: DownloadDetails
        A DownloadDetails named-tuple containing file-specific download information.
    partitions_to_download: List[PartitionDownloadDetails]
        A list of PartitionDownloadDetails named-tuples.

    Returns
    -------
    List[Any]
        If any missing partition is detected, the function will return a list of
        PartitionDownloadDetails named-tuples, one for each missing partition. If,
        instead, no missing partition is found, the function will return an empty
        list.
    """
    expected_partitions = get_partitions_download_details(
        partitions_to_download,
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
    partitions_to_download: List[PartitionDownloadDetails],
) -> List[PartitionDownloadDetails]:
    """Returns all the missing partitions from a download session.

    Parameters
    ----------
    whole_files_reference_data: List[DownloadDetails]
        A list of DownloadDetails named-tuples each containing file-specific download
        information.
    partitions_to_download: List[PartitionDownloadDetails]
        A list of PartitionDownloadDetails named-tuples.

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
        get_file_specific_missing_partitions(file, partitions_to_download)
        for file in get_partitioned_files(whole_files_reference_data)
        if len(get_file_specific_missing_partitions(file, partitions_to_download)) > 0
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
    files_with_missing_partitions: List[DownloadDetails],
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
        ),
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
    if len(available_partition_files) != 0:
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


def pre_concatenation_processing(
    download_manifest: ConcurrentDownloadManifest,
) -> ConcurrentDownloadManifest:
    """Implements the pre-concatenation processing phase.

    The pre-concatenation processing phase consist in checking the files that have been
    downloaded as a whole (because smaller than the multi-part threshold) for data
    integrity first. The files that failed the data integrity test are added to a new
    ConcurrentDownloadManifest that is used to collect the data of all those files or
    partitions that need to be downloaded again. Once the whole files are checked, the
    program proceeds with checking if all the partitions for all the files have been
    downloaded. If any file has a missing partition, the file specific information of the
    file is added to the ConcurrentDownloadManifest and the specific missing partitions
    are added as well to be downloaded once again.

    Parameters
    ----------
    download_manifest: ConcurrentDownloadManifest
        A ConcurrentDownloadManifest named-tuple containing the download manifest that
        was originally used to download the files concurrently.

    Returns
    -------
    ConcurrentDownloadManifest
        A ConcurrentDownloadManifest named-tuple containing the download manifest with
        the information necessary to download the whole files that were incorrectly
        downloaded in first place and the files that are missing some partitions.
    """
    failed_non_partitioned_files = get_list_of_failed_downloads(
        get_non_partitioned_files(download_manifest.files_reference_data),
    )
    missing_partitions = get_all_missing_partitions(
        whole_files_reference_data=download_manifest.files_reference_data,
        partitions_to_download=download_manifest.partitions_to_download,
    )
    files_with_missing_partitions = get_files_with_missing_partitions(
        whole_files_reference_data=download_manifest.files_reference_data,
        missing_partitions=missing_partitions,
    )
    files_to_retry_reference_data = list(
        itertools.chain(failed_non_partitioned_files, files_with_missing_partitions),
    )
    return ConcurrentDownloadManifest(
        files_reference_data=files_to_retry_reference_data,
        whole_files_to_download=failed_non_partitioned_files,
        partitions_to_download=missing_partitions,
    )


def concatenation_processing(
    download_manifest: ConcurrentDownloadManifest,
    failed_downloads_manifest: ConcurrentDownloadManifest,
) -> List[DownloadDetails]:
    """Implements the concatenation processing phase.

    During the concatenation processing phase, first all the information of all the files
    that are not missing any partition is collected, and then it is passed to the
    concatenating function that joins all the partitions of a specific-file to
    create a single compressed file.

    Parameters
    ----------
    download_manifest: ConcurrentDownloadManifest
        The download manifest containing all the information used originally to download
        the files.
    failed_downloads_manifest: ConcurrentDownloadManifest
        The download manifest containing the information of the files downloaded as a
        whole that failed the data integrity test, and of the files that were split in
        multiple partitions but that, after the initial download, were found missing one
        or more partitions.

    Returns
    -------
    List[DownloadDetails]
        A list containing the DownloadDetails named-tuples of all the files that went
        through the concatenation process.
    """
    files_with_missing_partitions = [
        file for file in failed_downloads_manifest.files_reference_data
        if file.is_partitioned is True
    ]
    files_ready_for_concatenation = get_files_ready_for_concatenation(
        whole_files_reference_data=download_manifest.files_reference_data,
        files_with_missing_partitions=files_with_missing_partitions,
    )
    return concatenate_each_file_partitions(files_ready_for_concatenation)


def update_failed_download_manifest(
    failed_downloads_manifest: ConcurrentDownloadManifest,
    initial_download_manifest: ConcurrentDownloadManifest,
    integrity_test_failing_files: List[DownloadDetails],
):
    """Updates the download manifest with the information of the files failing the integrity test.

    Parameters
    ----------
    failed_downloads_manifest: ConcurrentDownloadManifest
        The download manifest containing the information of the files downloaded as a
        whole that failed the data integrity test, and of the files that were split in
        multiple partitions but that, after the initial download, were found missing one
        or more partitions.
    initial_download_manifest: ConcurrentDownloadManifest
        The download manifest containing all the information used originally to download
        the files.
    integrity_test_failing_files: List[DownloadDetails]
        A list of DownloadDetails named-tuples of all those files that after concatenation
        failed the data integrity test (e.g. because one of the partitions was not
        downloaded properly).

    Returns
    -------
    ConcurrentDownloadManifest
        The download manifest containing the information of all those files whose
        download failed for different reasons (incomplete download, incomplete download
        of partitions, missing partitions etc.).
    """
    files_reference_data_to_update = copy.deepcopy(failed_downloads_manifest.files_reference_data)
    partition_to_download_to_update = copy.deepcopy(
        failed_downloads_manifest.partitions_to_download,
    )
    updated_file_reference = list(
        itertools.chain(
            files_reference_data_to_update,
            integrity_test_failing_files,
        ),
    )
    failed_partitions = []
    for failed_file in integrity_test_failing_files:
        file_specific_partitions = get_partitions_download_details(
            initial_download_manifest.partitions_to_download,
            failed_file.file_name,
        )
        failed_partitions.extend(file_specific_partitions)
    updated_partitions_to_download = list(itertools.chain(
        partition_to_download_to_update,
        list(itertools.chain(failed_partitions)),
    ))
    return ConcurrentDownloadManifest(
        files_reference_data=updated_file_reference,
        whole_files_to_download=failed_downloads_manifest.whole_files_to_download,
        partitions_to_download=updated_partitions_to_download,
    )


def post_concurrent_download_processing(
    download_manifest: ConcurrentDownloadManifest,
) -> ConcurrentDownloadManifest:
    """Implements the post concurrent download processing phase.

    Parameters
    ----------
    download_manifest: ConcurrentDownloadManifest
        The download manifest containing all the information used originally to download
        the files.

    Returns
    -------
    ConcurrentDownloadManifest
        The download manifest containing the information of the files that need to be
        downloaded once again.
    """
    initial_failed_downloads = pre_concatenation_processing(download_manifest)
    concatenated_files = concatenation_processing(download_manifest, initial_failed_downloads)
    integrity_test_failing_downloads = get_list_of_failed_downloads(concatenated_files)
    return update_failed_download_manifest(
        initial_failed_downloads,
        download_manifest,
        integrity_test_failing_downloads,
    )
