"""Implements the downloading functions."""

import concurrent.futures
from itertools import repeat
import pathlib
import threading

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


thread_local = threading.local()


def thread_get_session() -> requests.Session:
    """Creates a thread-specific session object.

    The thread-specific session object is required to ensure thread safety when using
    requests.Session(). When calling thread_get_session(), a session will be allocated
    exclusively to the thread that originally invoked the function. Once the session
    object is created, it will be reused by the thread on each subsequent call throughout
    its entire lifetime.

    Returns
    -------
    requests.Session
        A requests.Session object.
    """
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=5,
                    backoff_factor=0.1,
                    status_forcelist=(401, 500, 502, 503, 504),
                ),
            ),
        )
    return thread_local.session
