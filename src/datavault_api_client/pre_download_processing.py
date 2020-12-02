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
