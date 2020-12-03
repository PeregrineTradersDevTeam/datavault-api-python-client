"""Implements the functions used to process the crawler data before the download takes place.

The functions in this module process the raw information contained in the list of
DiscoveredFileInfo named tuples that is produced by the crawler, and prepare the download
manifest that is used by the downloading functions as a reference.
"""
import pathlib
from typing import Dict, Tuple, List, Union
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
    """Calculates the extremities of each partition and collects them in a list of dictionaries.

    Each dictionary contains a 'start' and 'end' key indicating the position in bytes of
    the starting and ending extremities of each partition.

    Parameters
    ----------
    file_size_in_bytes: int
        The size in Bytes of the file.
    partition_size_in_mib: float
        The size of the partitions in MiB

    Returns
    -------
    List[Dict[str, int]]
        A list of dictionaries containing the starting and ending extremities of each
        partition.
    """
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


def format_query_string(parameters_to_encode: Dict[str, int]) -> str:
    """Encodes a partition's extremities dictionary into a query string.

    The query string is formatted as:
        start=<partition-lower-extremity>&end=<partition-upper-extremity>

    Parameters
    ----------
    parameters_to_encode: Dict[str, int]
        A dictionary containing the lower and upper extremities of a partition, with the
        dictionary keys named 'start' and 'end'.

    Returns
    -------
    str
        A query string.
    """
    return urllib.parse.urlencode(parameters_to_encode)


def join_base_url_and_query_string(base_url: str, query_string: str) -> str:
    """Joins a query string to a base URL.

    Parameters
    ----------
    base_url: str
        The URL to which the query string is to be attached to.
    query_string: str
        A valid query string.

    Returns
    -------
    str
        The base url with attached the query string.
    """
    if base_url.endswith("/"):
        base_url = base_url[:-1]
    return f"{base_url}?{query_string}"


def create_partition_download_url(
    whole_file_download_url: str,
    partition_extremities: Dict[str, int],
) -> str:
    """Creates a partition-specific download URL.

    The created URL allows to download a specific partition of a file.

    Parameters
    ----------
    whole_file_download_url: str
        The download URL that is used to download a specific file.
    partition_extremities: Dict[str, int]
        A dictionary containing the lower and upper extremities of the partition to
        encode.

    Returns
    -------
    str
        The download URL for a specific partition.
    """
    return join_base_url_and_query_string(
        whole_file_download_url, format_query_string(partition_extremities),
    )


def generate_path_to_file_partition(
    path_to_whole_file: pathlib.Path,
    partition_index: int,
) -> pathlib.Path:
    """Creates a partition-specific path from the path of the whole and the partition index.

    Each partition gets a unique name that is a combination of the name of the whole
    file that the partition belongs to, and the partition index, or the position of the
    partition relative to the body of the whole file.

    Parameters
    ----------
    path_to_whole_file: pathlib.Path
        The full path to the location where the whole file that is partitioned should be
        saved.
    partition_index: int
        A numerical index that indicated where the specific partition falls in the order
        of all the same file partitions.

    Returns
    -------
    pathlib.Path
        The partition-specific full path.
    """
    return path_to_whole_file.parent.joinpath(
        (
            f"{path_to_whole_file.stem.split('.')[0]}_{(partition_index + 1)}"
            f"{path_to_whole_file.suffixes[0]}"
        ),
    )


def create_list_of_file_specific_partition_download_info(
    file_specific_download_info: DownloadDetails,
    partition_size_in_mib: float,
) -> List[PartitionDownloadDetails]:
    """Creates a file-specific list of PartitionDownloadDetails named-tuples.

    The PartitionDownloadDetails named-tuples contain partition-specific information that
    is used in the download phase. In particular, each named-tuple contains the name of
    the parent file (the file that is partitioned), the partition-specific download url
    and full path.

    Parameters
    ----------
    file_specific_download_info: DownloadDetails
        A DownloadDetails named tuple containing the file name as given by the DataVault
        API, the whole-file download URL, the file path, the size, the md5sum digest and
        the is_partitioned boolean flag.
    partition_size_in_mib: float
        The size of the partitions in MiB

    Returns
    -------
    List[PartitionDownloadDetails]
        A list of PartitionDownloadDetails named tuples, each containing the following
        partition-specific download information: name of the parent file,
        partition-specific download url, and full path to the partition.
    """
    file_partitions = calculate_list_of_partition_extremities(
        file_specific_download_info.size,
        partition_size_in_mib,
    )
    return [
        PartitionDownloadDetails(
            parent_file_name=file_specific_download_info.file_name,
            download_url=create_partition_download_url(
                file_specific_download_info.download_url,
                extremities,
            ),
            file_path=generate_path_to_file_partition(
                file_specific_download_info.file_path,
                partition_index,
            ),
        )
        for partition_index, extremities in enumerate(file_partitions)
    ]


def filter_files_to_split(
    whole_files_download_info: List[DownloadDetails],
) -> List[DownloadDetails]:
    """Creates a list of files that are eligible to be split in multiple partitions.

    Parameters
    ----------
    whole_files_download_info: List[DownloadDetails]
        A list of DownloadDetails named tuples containing the download information of
        all the files that we want to filter.

    Returns
    -------
    List[DownloadDetails]
        A list of DownloadDetails named tuples of all those files that had the
        is_partitioned flag set equal to True.
    """
    return [file for file in whole_files_download_info if file.is_partitioned is True]


def generate_whole_files_and_partitions_download_manifest(
    whole_files_download_info: List[DownloadDetails],
    partition_size_in_mib: float,
) -> List[Union[DownloadDetails, PartitionDownloadDetails]]:
    """Generates the download manifest containing whole files and partitions download information.

    When downloading files concurrently, the downloader function takes as an input a
    download manifest containing a mix of whole files download information (in the form
    of DownloadDetails named tuples) for all of those files that are smaller than the
    multi-part threshold, and partition files download details (in the form of
    PartitionDownloadDetails named tuples), for all those files that are larger than the
    multipart threshold and that are split in multiple parts to maximise the advantages of
    concurrency during the download.

    The function takes as an input the download manifest of all the whole files discovered
    by crawling the Datavault API directories, obtained by processing the raw0 download
    information generated by the crawler. It then processes each DownloadDetails named
    tuple by first checking through the `is_partitioned` flag if the file has to be
    split in multiple partitions or if instead has to be downloaded at once.
    If the file is smaller than the multi-part threshold (`is_partitioned` flag set to
    False), then the function stores the original DownloadDetails named tuple in the new
    download manifest. Conversely, if the file is larger than the multipart threshold
    (`is_partitioned` flag set to True), the function first calculate all the download
    details of its partitions and then store each PartitionDownloadDetails in the
    download manifest.

    Parameters
    ----------
    whole_files_download_info: List[DownloadDetails]
        A list of DownloadDetails named-tuples.
    partition_size_in_mib: float
        The size of the partitions in MiB.

    Returns
    -------
    List[Union[DownloadDetails, PartitionDownloadDetails]]
        A list of DownloadDetails and PartitionDownloadDetails named-tuples.
    """
    whole_files_and_partition_download_manifest = []
    for file_specific_download_info in whole_files_download_info:
        if file_specific_download_info.is_partitioned is False:
            whole_files_and_partition_download_manifest.append(file_specific_download_info)
        else:
            partitions_download_info = create_list_of_file_specific_partition_download_info(
                file_specific_download_info,
                partition_size_in_mib,
            )
            for partition_download_info in partitions_download_info:
                whole_files_and_partition_download_manifest.append(partition_download_info)
    return whole_files_and_partition_download_manifest


def prepare_download_manifests(
    discovered_files_info: List[DiscoveredFileInfo],
    path_to_data_folder: str,
    partition_size_in_mib: float = 5.0,
) -> Tuple[List[DownloadDetails], List[Union[DownloadDetails, PartitionDownloadDetails]]]:
    """Generates the whole-files and the whole-files and partitions download manifests.

    Parameters
    ----------
    discovered_files_info: List[DiscoveredFileInfo]
        A list of DiscoveredFileInfo named-tuples.
    path_to_data_folder: str
        The full path to the directory where the data will be downloaded.
    partition_size_in_mib: float
        The size of the partitions in MiB. By default is set equal to 5.0 MiB.

    Returns
    -------
    Tuple[List[DownloadDetails], List[Union[DownloadDetails, PartitionDownloadDetails]]]
        A tuple where the first element is a list of DownloadDetails named-tuples (the
        whole-files download manifest), and the second element is a list of both
        DownloadDetails and PartitionDownloadDetails named-tuples (the whole-files and
        partitions download manifest).
    """
    whole_files_download_manifest = process_all_discovered_files_info(
        discovered_files_info,
        path_to_data_folder,
        partition_size_in_mib,
    )
    files_and_partitions_download_manifest = generate_whole_files_and_partitions_download_manifest(
        whole_files_download_manifest,
        partition_size_in_mib,
    )
    return whole_files_download_manifest, files_and_partitions_download_manifest
