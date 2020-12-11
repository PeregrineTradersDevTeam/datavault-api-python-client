"""Collects the data structures used across the datavault_api_client library."""
import datetime
import pathlib
from typing import NamedTuple


class DiscoveredFileInfo(NamedTuple):
    """Characterises a file available to download discovered while crawling the DataVault API."""

    file_name: str
    download_url: str
    source_id: int
    reference_date: datetime.datetime
    size: int
    md5sum: str


class DownloadDetails(NamedTuple):
    """Contains all the information necessary to download and save a file from the DataVault API."""

    file_name: str
    download_url: str
    file_path: pathlib.Path
    source_id: int
    reference_date: datetime.datetime
    size: int
    md5sum: str
    is_partitioned: bool


class PartitionDownloadDetails(NamedTuple):
    """Contains all the information necessary to download and save a file partition."""

    parent_file_name: str
    download_url: str
    file_path: pathlib.Path
