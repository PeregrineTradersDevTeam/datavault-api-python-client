"""Implements a crawler for the Datavault API.

The crawler uses a depth-first search traversal algorithm to scan the directory tree
underlying a pre-defined Datavault API endpoint, to discover all the files available to
download in the tree underneath that specific endpoint.
"""

import re
from typing import Optional, Set
import urllib.parse

from datavault_api_client.connectivity import create_session


def clean_raw_filename(raw_filename: str) -> str:
    """Cleans a raw DataVault file name.

    The function deal with the different specification of file names within the DataVault
    platform. While COREREF, CROSSREF, CUSIP, PREMREF, REPLAY and SEDOL files have a
    naming convention consisting of the arrangement: '<FILE-TYPE>_<SOURCE-ID>_<DATE>',
    the same naming convention does not apply to WATCHLIST files, which instead have the
    user name as an additional term included between the <FILE-TYPE> and <SOURCE-ID>
    components of the name. To maintain the same name structure, the function selectively
    manipulates the passed raw file name: if the raw file name belongs to a WATCHLIST
    file, then the function will remove the user name from the file name and return a
    cleaned filename that respects the naming convention of the other file types, else,
    the function returns the raw filename that was originally passed, since it already
    respect the desired naming structure.

    Parameters
    ----------
    raw_filename: str
        A file name of a DataVault file.

    Returns
    -------
    str
        If raw_filename belongs to a WATCHLIST file, returns the cleaned file name with
        the naming structure '<FILE-TYPE>_<SOURCE-ID>_<DATE>'. If, instead, raw_filename
        belongs to any other DataVault file type, returns the passed file name.
    """
    if raw_filename.startswith("WATCHLIST"):
        filename_components = raw_filename.split("_")
        return "_".join([filename_components[0], filename_components[2], filename_components[3]])
    return raw_filename


def parse_source_from_file_name(file_name: str) -> str:
    """Parses the source id from a DataVault file name.

    Using the naming convention used across the different file types in the DataVault
    platform, that structures file names according to the '<FILE-TYPE>_<SOURCE-ID>_<DATE>'
    format, the function parses the passed file name and return the second element of
    the name, which contains the source.

    Parameters
    ----------
    file_name: str
        The file name of a DataVault file.

    Returns
    -------
    str
        The source id parsed from file_name
    """
    if not re.match(r"^[A-Z]+_[0-9]{3,4}_[0-9]{8}\.txt\.bz2$", file_name):
        raise Exception
    return file_name.split("_")[1]

