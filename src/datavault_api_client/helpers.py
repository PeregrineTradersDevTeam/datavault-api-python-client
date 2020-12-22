"""Implements helper functions."""


from typing import Tuple

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

