"""Implements the functions checking for the integrity of the downloaded data."""
import hashlib
import pathlib
from typing import List

from datavault_api_client.data_structures import DownloadDetails


def calculate_checksum(path_to_file: pathlib.Path, hash_constructor=hashlib.md5) -> str:
    """Calculates the checksum of a file, given a specific hash algorithm.

    Parameters
    ----------
    path_to_file: pathlib.Path
        A pathlib.Path object containing the full path to the file for which we want to
        calculate the checksum
    hash_constructor:
        An hashlib hash constructor indicating the hash algorithm to be used for
        calculating the checksum.

    Returns
    -------
    str
        The file checksum of the file as a string containing only hexadecimal digits.

    Examples
    --------
    If we want to obtain the md5 digest of a file:
    >>> md5_digest = calculate_checksum(path_to_file)

    To get the sha512 digest of a file:
    >>> sha512_digest = calculate_checksum(path_to_file, hash_constructor=hashlib.sha512)
    """
    file_hash = hash_constructor()
    optimal_chunk_size = file_hash.block_size * 128
    with path_to_file.open(mode="rb") as file:
        for chunk in iter(lambda: file.read(optimal_chunk_size), b""):
            file_hash.update(chunk)
    return file_hash.hexdigest()


def check_size(path_to_file: pathlib.Path) -> int:
    """Calculates the size of a file in bytes.

    Parameters
    ----------
    path_to_file: pathlib.Path
        The path to the file for which we want to calculate the size.

    Returns
    -------
    int
        The size of the file in bytes.

    """
    return path_to_file.stat().st_size


def data_integrity_test(file_download_details: DownloadDetails) -> bool:
    """Checks if the checksum digest and size of the downloaded file match the expected values.

    Parameters
    ----------
    file_download_details: DownloadDetails
        A DownloadDetails named tuple containing, among others, the expected
        characteristics of a specific file (size and md5sum digest).

    Returns
    -------
    bool
        True if the downloaded file passes the test, False otherwise.
    """
    test_results = (
        check_size(file_download_details.file_path) == file_download_details.size,
        calculate_checksum(file_download_details.file_path) == file_download_details.md5sum,
    )
    if all(test_results) is True:
        return True
    return False


def get_list_of_failed_downloads(
    downloaded_files_info: List[DownloadDetails],
) -> List[DownloadDetails]:
    """Tests the integrity of a list of files and collects those files that failed the test.

    Parameters
    ----------
    downloaded_files_info: List[DownloadDetails]
        A list of DownloadDetails named-tuples containing, for each file, the file name,
        the download URL, the file path, the file size, the md5sum and the is_partitioned
        flag.

    Returns
    -------
    List[DownloadDetails]
        A list of DownloadDetails named-tuples of all those files that failed the
        integrity test.

    """
    return [file for file in downloaded_files_info if data_integrity_test(file) is False]
