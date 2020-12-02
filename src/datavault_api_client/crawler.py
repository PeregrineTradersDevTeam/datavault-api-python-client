"""Implements a crawler for the Datavault API.

The crawler uses a depth-first search traversal algorithm to scan the directory tree
underlying a pre-defined Datavault API endpoint, to discover all the files available to
download in the tree underneath that specific endpoint.
"""
from typing import Dict, List, Optional, Tuple
import urllib.parse

import requests

from datavault_api_client.connectivity import create_session
from datavault_api_client.data_structures import DiscoveredFileInfo


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


def parse_source_from_name(file_name: str) -> str:
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
    #  if not re.match(r"^[A-Z]+_[0-9]{3,4}_[0-9]{8}\.txt\.bz2$", file_name):
    #  raise Exception
    return file_name.split("_")[1]


def create_discovered_file_object(file_node: Dict) -> DiscoveredFileInfo:
    """Creates a DiscoveredFileInfo named-tuple from the information in the leaf nodes of the API.

    Each leaf node of the DataVault API directory tree consists of a dictionary containing
    information describing the characteristics of a specific file (file name, file url
    path, file size, file checksum, among the others). The function extracts these pieces
    of information from the dictionary, enriches them, and collects them in a dedicated
    named-tuple.

    Parameters
    ----------
    file_node: Dict
        The dictionary obtained as a response to the API call at the instrument-type-level
        url of the DataVault API. The dictionary contains all the information that is
        necessary to describe a DataVault file.

    Returns
    -------
    DiscoveredFileInfo
        A named tuple that organises the file information retrieved through the API call.

    """
    return DiscoveredFileInfo(
        file_name=clean_raw_filename(file_node["name"]),
        download_url=create_node_url(file_node["url"]),
        source=parse_source_from_name(
            clean_raw_filename(file_node["name"]),
        ),
        size=file_node["size"],
        md5sum=file_node["md5sum"],
    )


def initialise_search(
    url: str,
    credentials: Tuple[str, str],
    session: requests.Session,
    source_id: Optional[str] = None,
) -> Tuple[List, List[DiscoveredFileInfo]]:
    """Initialises the tree search by discovering the child nodes of the passed url.

    Parameters
    ----------
    url: str
        The url used as the starting point of the API directory tree search.
    credentials: Tuple[str, str]
        A tuple containing the username and password used to access the DataVault API.
    session: requests.Session
        A session object.
    source_id: Optional[str]
        An optional string that allows to specify a specific source id for which we
        want to discover the available files to download.

    Returns
    -------
    Tuple[List, List[DiscoveredFileInfo]]
        A tuple containing the stack list and the leaf_nodes list. The stack list
        contains the details of the neighbour nodes that are directories, while the
        leaf_nodes list contains the DiscoveredFileInfo named-tuples with the details of
        the files discovered in the child nodes. Normally, unless the url passed to the
        function is an instrument-type-level url, the leaf_nodes list will be empty,
        while the stack list will be populated with the information of the child nodes
        of the passed url.
    """
    stack = []
    leaf_nodes = []
    with session.get(url, auth=credentials) as initial_response:
        # if initial_response.status_code == 200:
        initial_response.raise_for_status()
        for neighbour in initial_response.json():
            if neighbour["directory"] is True:
                stack.append(neighbour)
            else:
                if not source_id:
                    leaf_nodes.append(create_discovered_file_object(neighbour))
                else:
                    if (
                        parse_source_from_name(
                            clean_raw_filename(neighbour["name"]),
                        ) == source_id
                    ):
                        leaf_nodes.append(create_discovered_file_object(neighbour))
    return stack, leaf_nodes


def create_node_url(url_path: str) -> str:
    """Creates a full node url by joining the url path with the other url components.

    Parameters
    ----------
    url_path: str
        A url path pointing to the location of the node within the API directory tree.

    Returns
    -------
    str
        The full node url obtained by combining the url path with the other components
        of the url.
    """
    return urllib.parse.urljoin(
        "https://api.icedatavault.icedataservices.com",
        url_path,
    )


def traverse_api_directory_tree(
    session: requests.Session,
    credentials: Tuple[str, str],
    stack: List,
    leaf_nodes: List[DiscoveredFileInfo],
    source_id: Optional[str] = None,
) -> List[DiscoveredFileInfo]:
    """Transverses the DataVault API directory tree and returns the discovered files.

    Parameters
    ----------
    session: requests.Session
        A session object.
    credentials: Tuple[str, str]
        A tuple containing the username and password used to access the DataVault API.
    stack: List
        A list of the details of the neighbour nodes that are directories as returned by
        the DataVault API.
    leaf_nodes: List[DiscoveredFileInfo]
        A list of DiscoveredFileInfo named-tuples with the details of the files eventually
        discovered during initialisation.
    source_id: Optional[str]
        A source id as a string. It controls what files are included in the returned list
        of DiscoveredFileInfo named-tuples. If not specified, all the discovered files are
        returned. If source_id is, instead, specified, only the files that belong to the
        specified source are included in the list.

    Returns
    -------
    List[DiscoveredFileInfo]
        A list of DiscoveredFileInfo named-tuples with the details of the files discovered
        while traversing the directory tree of the DataVault API.
    """
    visited_nodes = []
    while len(stack) != 0:
        node_to_visit = stack.pop()
        if node_to_visit not in visited_nodes:
            visited_nodes.append(node_to_visit)
            url_to_query = create_node_url(node_to_visit["url"])
            with session.get(url_to_query, auth=credentials) as response:
                # if response.status_code == 200:
                response.raise_for_status()
                discovered_neighbours = response.json()
                for neighbour in discovered_neighbours:
                    if neighbour["directory"] is True:
                        stack.append(neighbour)
                    else:
                        if not source_id:
                            leaf_nodes.append(
                                create_discovered_file_object(neighbour),
                            )
                        else:
                            if (
                                parse_source_from_name(
                                    clean_raw_filename(neighbour["name"]),
                                ) == source_id
                            ):
                                leaf_nodes.append(
                                    create_discovered_file_object(neighbour),
                                )
    return leaf_nodes


def datavault_crawler(
    url: str, credentials: Tuple[str, str], source_id: Optional[str] = None,
) -> List[DiscoveredFileInfo]:
    """Crawls the directory tree of the DataVault API to discover files available to download.

    Parameters
    ----------
    url: str
        The url from which the crawler will start traversing the directory tree.
    credentials: Tuple[str, str]
        A tuple containing the username and password used to access the DataVault API.
    source_id: Optional[str]
        An optional string that allows to specify a specific source id for which we
        want to discover the available files to download.

    Returns
    -------
    List[DiscoveredFileInfo]
        A list containing DiscoveredFileInfo named-tuples with the download information
        of each of the discovered files available to download.
    """
    session = create_session()
    stack, leaf_nodes = initialise_search(url, credentials, session, source_id)
    return traverse_api_directory_tree(session, credentials, stack, leaf_nodes, source_id)
