"""Implements helper functions."""

from typing import List, Tuple

from datavault_api_client.data_structures import DiscoveredFileInfo


##########################################################################################


class MissingOnyxCredentialsError(Exception):
    """A class for an exception to raise in case of missing credentials."""


class InvalidOnyxCredentialTypeError(Exception):
    """A class for an exception to raise in case the credentials are not of the type string."""


def validate_credentials_type(credentials: Tuple[str, str]) -> None:
    """Checks if the credentials have the right type attribute.

    Parameters
    ----------
    credentials: Tuple[str, str]
        A tuple containing the username and the password to validate.

    Raises
    ------
    InvalidOnyxCredentialTypeError
    """
    if any(type(credential) is not str for credential in credentials):
        raise InvalidOnyxCredentialTypeError("Invalid credentials types.")


def validate_credentials_presence(credentials: Tuple[str, str]) -> None:
    """Checks for any missing credential.

    Parameters
    ----------
    credentials: Tuple[str, str]
        A tuple containing the username and the password to validate.

    Raises
    ------
    MissingOnyxCredentialsError
    """
    username, password = credentials
    if all(credential is None for credential in credentials):
        raise MissingOnyxCredentialsError('Missing username and password')
    elif any(credential is None for credential in credentials):
        if username is None:
            raise MissingOnyxCredentialsError('Missing username')
        else:
            raise MissingOnyxCredentialsError('Missing password')


def validate_credentials(credentials: Tuple[str, str]) -> None:
    """Validates the credentials.

    Parameters
    ----------
    credentials: Tuple[str, str]
        A tuple containing the username and the password to validate.
    """
    if not validate_credentials_presence(credentials):
        validate_credentials_type(credentials)


##########################################################################################


def calculate_number_of_discovered_files(discovered_files: List[DiscoveredFileInfo]) -> int:
    """Calculates the number of files discovered by the DataVault crawler.

    Parameters
    ----------
    discovered_files: List[DiscoveredFileInfo]
        A list of DiscoveredFileInfo named-tuples containing the file-specific information
        of each file that was discovered by the DataVault crawler.

    Returns
    -------
    int
        The total number of files discovered by the DataVault crawler.
    """
    return len(discovered_files)


def generate_human_readable_size(byte_size: int) -> str:
    """Generate a human readable size from a byte size.

    Returns a human readable string with the size converted in one of the multiple of the byte
    according to the standards defined by the International Electrotechnical Commission (IEC) in
    1998. Available multiples are kibibytes (1024 bytes, KiB), mibibytes (1024^2 bytes, MiB),
    gibibytes (1024^3 bytes, GiB) and tibibytes (1024^4 bytes, TiB).

    Parameters
    ----------
    byte_size: int
        The size in bytes to convert.

    Returns
    -------
    str
        The converted byte size, followed by the right suffix.

    """
    size_measurement_units = (('KiB', 1024), ('MiB', 1024**2), ('GiB', 1024**3), ('TiB', 1024**4))
    suffix = None
    divisor = None
    for u, m in size_measurement_units:
        if byte_size >= m:
            suffix = u
            divisor = m

    if suffix and divisor:
        return f'{round(byte_size / divisor, 1)} {suffix}'
    return f'{byte_size}B'
    # return f'{round(byte_size/divisor, 1)} {suffix}'


def calculate_total_download_size(discovered_files: List[DiscoveredFileInfo]) -> str:
    """Calculate the total download size and returns the size in a human readable format.

    Parameters
    ----------
    discovered_files: List[DiscoveredFileInfo]
        A list of DiscoveredFileInfo named-tuples containing the file-specific information
        of each file that was discovered by the DataVault crawler.

    Returns
    -------
    str
        The total download size in a human readable form.
    """
    file_sizes = [discovered_file.size for discovered_file in discovered_files]
    total_download_size = sum(file_sizes)
    return generate_human_readable_size(total_download_size)
