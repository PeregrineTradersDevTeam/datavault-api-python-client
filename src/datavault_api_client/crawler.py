"""Implements a crawler for the Datavault API.

The crawler uses a depth-first search traversal algorithm to scan the directory tree
underlying a pre-defined Datavault API endpoint, to discover all the files available to
download in the tree underneath that specific endpoint.
"""

import urllib.parse
from typing import Optional, Set

from datavault_api_client.connectivity import create_session


def clean_raw_watchlist_filename(raw_filename: str) -> str:
    if raw_filename.startswith("WATCHLIST"):
        filename_components = raw_filename.split("_")
        return "_".join([filename_components[0], filename_components[2], filename_components[3]])
    return raw_filename


