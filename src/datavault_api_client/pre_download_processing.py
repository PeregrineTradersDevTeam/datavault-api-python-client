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


def calculate_multi_part_threshold(partition_size_in_mib: float) -> int:
    """Calculates the file size above which a file is to be split in same size partitions.

    The multi-part threshold is calculated as 2 times the partition size in bytes, plus an
    additional buffer of 80% the partition size in bytes. By using this threshold, only
    files that have at least two same-size partitions and one additional partition that
    is at least as large as 80% of the partition size are actually split into multiple
    partitions.

    Parameters
    ----------
    partition_size_in_mib: float
        The partition size in MiB.

    Returns
    -------
    int
        The multi-part threshold in Bytes.
    """
    return round(
        (convert_mib_to_bytes(partition_size_in_mib) * 2) + (0.8 * convert_mib_to_bytes(
            partition_size_in_mib,
        )),
    )


def check_if_partitioned(file_size_in_bytes: int, partition_size_in_mib: float) -> bool:
    """Checks whether a file size exceeds the multi-part threshold.

    Parameters
    ----------
    file_size_in_bytes: int
        The file size of the file in Bytes.
    partition_size_in_mib: float
        The desired partition's size in MiB.

    Returns
    -------
    bool
        True if the file size is larger than the multi-part threshold, False otherwise.
    """
    if file_size_in_bytes >= calculate_multi_part_threshold(partition_size_in_mib):
        return True
    return False


def process_raw_download_info(
    raw_download_info: DiscoveredFileInfo,
    path_to_data_folder: str,
    partition_size_in_mib: float,
) -> DownloadDetails:
    """Process the raw download information contained in a DiscoveredFileInfo named-tuple.

    The information contained in a DiscoveredFileInfo named-tuple is not comprehensive
    enough for the download phase. For this reason, this information is enriched with
    information that is specifically designed to facilitate the download process. The
    function adds to the information already contained in the DiscoveredFileInfo object,
    the full path to the location where the file has to be downloaded (the path respects
    the structure of the DataVault API directory tree) and a is_partitioned flag that
    informs whether a file is large enough to require partitioning in case of concurrent
    download or not.

    Parameters
    ----------
    raw_download_info: DiscoveredFileInfo
        A DiscoveredFileInfo containing the information that characterise a file in the
        DataVault API.
    path_to_data_folder: str
        The full path to the directory where the file will be downloaded.
    partition_size_in_mib: float
        The partition size in MiB.

    Returns
    -------
    DownloadDetails
        A DownloadDetails named tuple containing the file name, the download url, the file
        path where the file will be downloaded, the file size, the md5sum digest, and a
        flag that informs whether the file is eligible to be split in multiple partitions.
    """
    return DownloadDetails(
        file_name=raw_download_info.file_name,
        download_url=raw_download_info.download_url,
        file_path=generate_file_path_matching_datavault_structure(
            path_to_data_folder,
            file_name=raw_download_info.file_name,
            datavault_download_url=raw_download_info.download_url,
        ),
        size=raw_download_info.size,
        md5sum=raw_download_info.md5sum,
        is_partitioned=check_if_partitioned(raw_download_info.size, partition_size_in_mib),
    )


def process_all_discovered_files_info(
    discovered_files_info: List[DiscoveredFileInfo],
    path_to_data_directory: str,
    partition_size_in_mib: float,
) -> List[DownloadDetails]:
    """Process the raw download details of all the files discovered by the crawler.

    Parameters
    ----------
    discovered_files_info: List[DiscoveredFileInfo]
        The list of DiscoveredFileInfo named-tuples produced by the DataVault crawler and
        containing the raw download information of each discovered file.
    path_to_data_directory: str
        The path to the directory where the data will be downloaded.
    partition_size_in_mib: float
        The size of the partitions in MiB.

    Returns
    -------
    List[DownloadDetails]
        A list of DownloadDetails named-tuples each containing all the information
        necessary to download a specific discovered file.
    """
    return [
        process_raw_download_info(file_info, path_to_data_directory, partition_size_in_mib)
        for file_info in discovered_files_info
    ]


def calculate_number_of_same_size_partitions(
    file_size_in_bytes: int,
    partition_size_in_mib: float,
) -> int:
    """Calculates the number of same size partitions given file size and partition size.

    Parameters
    ----------
    file_size_in_bytes: int
        The size in Bytes of the file.
    partition_size_in_mib: float
        The size of the partitions in MiB.

    Returns
    -------
    int
        The number of same size partitions in the file.

    """
    return file_size_in_bytes // convert_mib_to_bytes(partition_size_in_mib)


def calculate_size_of_last_partition(file_size_in_bytes: int, partition_size_in_mib: float) -> int:
    """Calculates the size of the last file's partition.

    The last partition is defined as the bytes that exceed the n same-size partitions in
    which a file gets split into, when the n same-size partitions leave a remainder of
    bytes to cover in a separate partition.

    Parameters
    ----------
    file_size_in_bytes: int
        The size in Bytes of the file.
    partition_size_in_mib: float
        The size of the partitions in MiB

    Returns
    -------
    int
        The size in Bytes of the last partition.

    """
    return file_size_in_bytes % convert_mib_to_bytes(partition_size_in_mib)


def calculate_list_of_partition_upper_extremities(
    file_size_in_bytes: int,
    partition_size_in_mib: float,
) -> List[int]:
    """Calculates the upper partition's extremities and collects them in a list.

    The upper partition's extremities indicates the position, in bytes, where a partition
    ends.

    Parameters
    ----------
    file_size_in_bytes: int
        The size in Bytes of the file.
    partition_size_in_mib: float
        The size of the partitions in MiB

    Returns
    -------
    List[int]
        A list of bytes indicating the upper extremities of each partition in a file.
    """
    list_of_upper_extremities = [
        convert_mib_to_bytes(partition_size_in_mib) * i
        for i in range(1, calculate_number_of_same_size_partitions(
            file_size_in_bytes, partition_size_in_mib,
        ) + 1)
    ]

    if calculate_size_of_last_partition(file_size_in_bytes, partition_size_in_mib) != 0:
        list_of_upper_extremities.append(file_size_in_bytes)

    return list_of_upper_extremities


def calculate_list_of_partition_lower_extremities(
    file_size_in_bytes: int,
    partition_size_in_mib: float,
) -> List[int]:
    """Calculates the lower partition's extremities and collects them in a list.

    The lower partition's extremities indicates the position, in bytes, where a partition
    starts.

    Parameters
    ----------
    file_size_in_bytes: int
        The size in Bytes of the file.
    partition_size_in_mib: float
        The size of the partitions in MiB

    Returns
    -------
    List[int]
        A list of bytes indicating the lower extremities of each partition in a file.
    """
    list_of_lower_extremities = [0]
    list_of_lower_extremities += [
        upper_extremity + 1 for upper_extremity in calculate_list_of_partition_upper_extremities(
            file_size_in_bytes, partition_size_in_mib,
        ) if upper_extremity < file_size_in_bytes
    ]
    return list_of_lower_extremities


def calculate_list_of_partition_extremities(
    file_size_in_bytes: int,
    partition_size_in_mib: float,
) -> List[Dict[str, int]]:
    return [
        {"start": extremities[0], "end": extremities[1]}
        for extremities in zip(
            calculate_list_of_partition_lower_extremities(
                file_size_in_bytes, partition_size_in_mib,
            ),
            calculate_list_of_partition_upper_extremities(
                file_size_in_bytes, partition_size_in_mib,
            ),
        )
    ]
