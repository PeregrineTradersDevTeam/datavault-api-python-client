"""Module containing the command line app."""
import click
import sys
import time

from datavault_api_client.crawler import datavault_crawler
from datavault_api_client.downloaders import (
    download_files_concurrently,
    download_files_synchronously,
)
from datavault_api_client.pre_download_processing import (
    pre_concurrent_download_processor,
    pre_synchronous_download_processor,
)
import datavault_api_client.helpers
from datavault_api_client.helpers import validate_credentials, calculate_number_of_discovered_files, calculate_total_download_size


@click.group()
def datavault():
    pass


@datavault.command(name="get")
@click.argument("datavault_endpoint", type=click.STRING)
@click.argument("data_directory", type=click.Path(exists=True))
@click.option("--username", "-u", type=click.STRING, envvar="ICE_API_USERNAME")
@click.option("--password", "-p", type=click.STRING, envvar="ICE_API_PASSWORD")
@click.option("--concurrent", "download_type", flag_value="concurrent", default=True)
@click.option("--synchronous", "download_type", flag_value="synchronous")
@click.option("--source", "-s", type=click.STRING, default=None)
@click.option("--partition-size", type=click.FLOAT, default=5.0)
@click.option("--num-workers", type=click.INT)
@click.option("--max-download-attempts", type=int, default=5)
def get(
    datavault_endpoint,
    data_directory,
    username,
    password,
    download_type,
    source,
    partition_size,
    num_workers,
    max_download_attempts,
):
    credentials = (username, password)
    try:
        validate_credentials(credentials)
    except datavault_api_client.helpers.MissingOnyxCredentialsError as missing_credentials_error:
        click.echo(repr(missing_credentials_error))
        sys.exit("Process finished with exit code 1")
    except datavault_api_client.helpers.InvalidOnyxCredentialTypeError as invalid_type_error:
        click.echo(repr(invalid_type_error))
        sys.exit("Process finished with exit code 1")

    click.echo("Initialising the DataVault Crawler ...")
    click.echo("Searching for files to download ...")
    discovered_files_to_download = datavault_crawler(
        datavault_endpoint,
        credentials,
        source_id=source,
    )
    click.echo(
        f"Discovered {calculate_number_of_discovered_files(discovered_files_to_download)} "
        f"file(s) to download."
    )
    click.echo(
        f"Total download size: {calculate_total_download_size(discovered_files_to_download)}"
    )
    time.sleep(2)

    if calculate_number_of_discovered_files(discovered_files_to_download) == 0:
        sys.exit("Process finished with exit code 0")
    else:
        if download_type == "synchronous":
            download_manifest = pre_synchronous_download_processor(
                discovered_files_to_download,
                data_directory,
            )
            click.echo("Initialising download ...")
            download_files_synchronously(
                download_manifest,
                credentials,
                max_number_of_download_attempts=max_download_attempts,
            )
        else:
            download_manifest = pre_concurrent_download_processor(
                discovered_files_to_download,
                path_to_data_directory=data_directory,
                partition_size_in_mib=partition_size,
            )
            click.echo("Initialising download ...")
            download_files_concurrently(
                download_manifest,
                credentials,
                max_number_of_workers=num_workers,
                max_number_of_download_attempts=max_download_attempts,
            )
        sys.exit("Process finished with exit code 0")


if __name__ == "__main__":
    datavault()
