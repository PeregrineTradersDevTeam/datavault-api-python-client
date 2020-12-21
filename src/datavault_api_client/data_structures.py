"""Collects the data structures used across the datavault_api_client library."""
import datetime
import pathlib
from typing import List, NamedTuple, Optional, TypedDict


class DiscoveredFileInfo(NamedTuple):
    """Characterises a file available to download discovered while crawling the DataVault API.

    The file_name field indicates the name of the file as given by the Datavault platform.
    The download_url field contains the file-specific download url.
    The source_id field refers to the market source ID from which the file originates.
    The reference_date is a datetime.datetime object with the date on which the data
    within the file was originally created.
    The size field indicates the size in bytes of the file to download.
    The md5sum field is the unique md5 checksum of the file.
    """

    file_name: str
    download_url: str
    source_id: int
    reference_date: datetime.datetime
    size: int
    md5sum: str


class DownloadDetails(NamedTuple):
    """Contains all the information necessary to download and save a file from the DataVault API.

    The file_name field indicates the name of the file as given by the Datavault platform.
    The download_url field contains the file-specific download url.
    The file_path field is a pathlib.Path object containing the full path to the location
    where the file will be downloaded.
    The source_id field refers to the market source ID from which the file originates.
    The reference_date is a datetime.datetime object with the date on which the data
    within the file was originally created.
    The size field indicates the size in bytes of the file to download.
    The md5sum field is the unique md5 checksum of the file.
    The is_partitioned flag is a boolean flag that indicates whether a file is larger than
    the multi-part threshold. In case the files will be downloaded synchronously, this
    field is equal to None as no multi-part threshold is used in the synchronous download
    setup.
    """

    file_name: str
    download_url: str
    file_path: pathlib.Path
    source_id: int
    reference_date: datetime.datetime
    size: int
    md5sum: str
    is_partitioned: Optional[bool]


class ItemToDownload(TypedDict):
    """Represents a file reference data in a dictionary form.

    This typed dictionary is used to encode the information contained in a
    DownloadDetails named tuple in a dictionary, to then convert the dictionary in
    a JSON object that is saved to file so to store a record of each file downloaded
    on a specific data that can be used later as a source of truth.
    """

    file_name: str
    download_url: str
    file_path: str
    source_id: int
    reference_date: str
    size: int
    md5sum: str


class PartitionDownloadDetails(NamedTuple):
    """Contains all the information necessary to download and save a file partition.

    The parent_file_name field indicates the name of the file the partition correspond to.
    The download_url field contains the partition-specific download url.
    The file_path field is a pathlib.Path object pointing to the location where the
    partition will be downloaded.
    The partition_index field indicates the index of the specific partition within the
    set of partitions that correspond to a specific file.
    """

    parent_file_name: str
    download_url: str
    file_path: pathlib.Path
    partition_index: int


class ConcurrentDownloadManifest(NamedTuple):
    """Implements a download manifest for the concurrent download setting.

    The files_reference_data field contains a list of DownloadDetails named-tuples that
    are used as a source of truth in the post-download processing phase to check file
    specific information.

    The whole_files_to_download filed contains a list of DownloadDetails named-tuples of
    all those files that are smaller than the multi-part threshold and that are therefore
    downloaded as a whole.

    The partitions_to_download filed contains a list of PartitionDownloadDetails named-tuples
    each corresponding to a specific file partition and carrying the information to
    download that unique partition.
    """

    files_reference_data: List[DownloadDetails]
    whole_files_to_download: List[DownloadDetails]
    partitions_to_download: List[PartitionDownloadDetails]
