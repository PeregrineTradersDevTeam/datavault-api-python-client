"""Implements the downloading functions."""

import concurrent.futures
import itertools
from itertools import repeat
import pathlib
import threading
from typing import List, Tuple

import click
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from datavault_api_client.connectivity import create_session
from datavault_api_client.data_integrity import get_list_of_failed_downloads
from datavault_api_client.data_structures import ConcurrentDownloadManifest, DownloadDetails
from datavault_api_client.post_download_processing import post_concurrent_download_processing


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


def download_file(download_info: DownloadDetails, credentials: tuple, session: requests.Session):
    download_url = download_info.download_url
    file_path = download_info.file_path
    pathlib.Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    # TODO: add logging to the function instead of using click.echo().
    click.echo(f"# Downloading {download_url} ...")
    with session.get(download_url, auth=credentials, stream=True) as response:
        # TODO: think about inserting a try-except block instead of using the status code check.
        if response.status_code == 200:
            with file_path.open("wb") as output:
                for chunk in response.iter_content(chunk_size=3 * 1024 * 1024):
                    output.write(chunk)
    # TODO: add logging to the function instead of using click.echo()
    click.echo(f"+ Download completed: {pathlib.Path(file_path).as_posix()}")


def download_files_synchronously(
    download_manifest: List[DownloadDetails],
    credentials: Tuple[str, str],
    max_number_of_download_attempts: int = 5,
    current_attempt=None,
) -> None:
    if current_attempt is None:
        current_attempt = 1

    session = create_session()
    for file in download_manifest:
        download_file(file, credentials, session)

    failed_downloads = get_list_of_failed_downloads(download_manifest)

    if len(failed_downloads) > 0:
        if current_attempt <= max_number_of_download_attempts:
            download_files_synchronously(
                failed_downloads,
                credentials,
                max_number_of_download_attempts=max_number_of_download_attempts,
                current_attempt=current_attempt+1,
            )
        else:
            for failed_download in failed_downloads:
                # TODO: add logging to the function instead of using click.echo()
                click.echo(f"- Failed to download: {failed_download.file_name}")
    else:
        # TODO: add logging to the function instead of using click.echo()
        click.echo("All files successfully downloaded.")


def thread_safe_download(download_info: DownloadDetails, credentials: Tuple[str, str]):
    session = thread_get_session()
    download_file(download_info, credentials, session)


def download_files_concurrently(
    concurrent_download_manifest: ConcurrentDownloadManifest,
    credentials: Tuple[str, str],
    max_number_of_workers: int = None,
    max_number_of_download_attempts: int = 5,
    current_attempt: int = None,
) -> None:
    if current_attempt is None:
        current_attempt = 1

    files_to_download = list(itertools.chain(
        concurrent_download_manifest.whole_files_to_download,
        concurrent_download_manifest.partitions_to_download,
    ))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_number_of_workers) as executor:
        executor.map(
            thread_safe_download,
            files_to_download,
            repeat(credentials),
        )

    failed_downloads = post_concurrent_download_processing(concurrent_download_manifest)

    if len(failed_downloads.files_reference_data) > 0:
        click.echo(f'Failed to download {len(failed_downloads.files_reference_data)} file(s).')
        if current_attempt <= max_number_of_download_attempts:
            download_files_concurrently(
                failed_downloads,
                credentials,
                max_number_of_workers=max_number_of_workers,
                max_number_of_download_attempts=max_number_of_download_attempts,
                current_attempt=current_attempt + 1)
        else:
            for failed_download in failed_downloads.files_reference_data:
                click.echo(f'- Failed to download: {failed_download.file_name}')
    else:
        click.echo('All files successfully downloaded.')
