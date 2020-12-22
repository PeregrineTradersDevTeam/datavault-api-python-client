"""Module containing the command line app."""
import click

from datavault_api_client.crawler import datavault_crawler
from datavault_api_client.downloaders import (
    download_files_concurrently,
    download_files_synchronously,
)
from datavault_api_client.pre_download_processing import (
    pre_concurrent_download_processor,
    pre_synchronous_download_processor,
)
