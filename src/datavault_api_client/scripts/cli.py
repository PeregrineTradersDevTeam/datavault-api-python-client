"""Module containing the command line app."""
import sys
import time

import click

from datavault_api_client.crawler import datavault_crawler
from datavault_api_client.downloaders import (
    download_files_concurrently,
    download_files_synchronously,
)
import datavault_api_client.helpers
from datavault_api_client.helpers import (
    calculate_number_of_discovered_files,
    calculate_total_download_size,
    validate_credentials,
)
from datavault_api_client.pre_download_processing import (
    pre_concurrent_download_processor,
    pre_synchronous_download_processor,
)


@click.group()
def datavault():
    """datavault is a command line interface to interact with the DataVault API."""
    pass


@datavault.command(name="get")
@click.argument("datavault_endpoint", type=click.STRING)
@click.argument("root_directory", type=click.Path(exists=True))
@click.option(
    "--username",
    "-u",
    type=click.STRING,
    envvar="ICE_API_USERNAME",
    help="The username used to access the DataVault API.",
)
@click.option(
    "--password",
    "-p",
    type=click.STRING,
    envvar="ICE_API_PASSWORD",
    help="The password used to access the DataVault API.",
)
@click.option(
    "--concurrent",
    "download_type",
    flag_value="concurrent",
    default=True,
    help=(
        "Influence the type of download executed by the program. If set to "
        "'concurrent', the program will download the files concurrently. This is the "
        "default setting of the program."
    ),
)
@click.option(
    "--synchronous",
    "download_type",
    flag_value="synchronous",
    help=(
        "Influence the type of download executed by the program. If the flag is set to "
        "'synchronous', the program will download the files synchronously, and only one "
        "file at a time will be downloaded."
    ),
)
@click.option(
    "--source",
    "-s",
    type=click.STRING,
    default=None,
    help=(
        "Select a specific source ID. If set, only the data belonging to the specific "
        "source id will be downloaded."
    ),
)
@click.option(
    "--partition-size",
    type=click.FLOAT,
    default=5.0,
    help=(
        "Specify the partition size in which the files are split when downloaded "
        "concurrently. If omitted, the partition size is set by default to 5 MiB. "
        "This command is only used when attempting to download files concurrently."
    )
)
@click.option(
    "--num-workers",
    type=click.INT,
    help=(
        "Specify the number of workers to be used by the concurrent download executor. "
        "If omitted, the executor will set the number of workers automatically to the "
        "minimum between 32 and the number of CPUs in the system being used plus 4. In "
        "this way, at least 5 workers are preserved for I/O bound tasks, and no more than "
        "32 CPU cores are used for CPU bound tasks, thus avoiding using very large "
        "resources implicitly on many-core machines. This command is only used when "
        "attempting to download files concurrently."
    ),
)
@click.option(
    "--max-download-attempts",
    type=int,
    default=5,
    help=(
        "Specify the maximum number of download attempts. This value is used when "
        "attempting to download files whose download failed in first place."
    ),
)
def get(
    datavault_endpoint,
    root_directory,
    username,
    password,
    download_type,
    source,
    partition_size,
    num_workers,
    max_download_attempts,
):
    """Discovers and downloads files from the DataVault API server.

    This command takes an endpoint of the DataVault API and a full path to a data
    directory where to save the downloaded data, and proceeds first to discover the files
    available to download in all the child directories of the DataVault endpoint, and then
    downloads each file to the indicated location on the local file system. Before exiting,
    the program checks if all the data matches the expected characteristics as originally
    retrieved from the DataVault API and, if any file fails this data integrity test,
    proceeds with downloading once again the failing files.

    The function allows the user to specify the type to download by selecting the
    'concurrent' or 'synchronous' download type flags. If the download type is set to
    'concurrent' the program will calculate the multi-part threshold given the specified
    partition size (if omitted it defaults to 5 MiB) and will split all the files that
    are larger than the threshold in multiple partitions. The files smaller than the threshold
    are downloaded as a whole. Once all the partitions to download are identified, the
    program proceeds with downloading partitions and whole files concurrently. At the end
    of the download the file-specific partitions are concatenated together in single file.
    If, instead, the download type flag is set to 'synchronous', the program will proceed
    with downloading a file at a time. For machines with limited bandwidth available, we
    recommend the usage of the 'synchronous' download type.

    \b
    Positional arguments:
    \b
    DATAVAULT_ENDPOINT          URL of the DataVault API endpoint to query.
    ROOT_DIRECTORY              Full path to the directory where the data will be downloaded.
    """
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
                root_directory,
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
                path_to_data_directory=root_directory,
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
