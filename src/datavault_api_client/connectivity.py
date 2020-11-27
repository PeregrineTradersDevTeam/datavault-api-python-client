"""Implements a connectivity function used across the library.

This module implements a function designed to create a Session object with support for a
retry logic that is triggered in case of occurrence of specific error status codes that
are generated as a response to the API call. The function is used across the different
modules of the watchlist_api_client library when a connection is to be reused by multiple
calls to the server (for example when crawling the API directory structure or when
downloading data from the server).
"""
from typing import Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


def create_session(
    total_retries: int = 5,
    backoff_factor: float = 0.1,
    status_forcelist: Tuple[int, ...] = (401, 500, 502, 503, 504),
) -> requests.Session:
    """Creates a session object with support for a retry logic.

    Parameters
    ----------
    total_retries: int
        The maximum number of retries allowed.
    backoff_factor: float
        The backoff factor used to calculate the waiting time between each retry.
    status_forcelist: tuple
        A tuple of status codes that will trigger a retry in case of occurrence.

    Returns
    -------
    requests.Session
        A requests.Session object.
    """
    session = requests.Session()
    session.mount(
        "https://",
        HTTPAdapter(
            max_retries=Retry(
                total=total_retries,
                backoff_factor=backoff_factor,
                status_forcelist=status_forcelist,
            ),
        ),
    )
    return session
