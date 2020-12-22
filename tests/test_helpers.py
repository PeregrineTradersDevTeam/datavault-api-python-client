import pytest

import datavault_api_client.helpers as helpers


class TestValidateCredentialsType:
    def test_validation_of_type_with_correct_credentials_tuple(self):
        # Setup
        credentials = ("Username", "Password")
        # Exercise
        # Verify
        assert helpers.validate_credentials_type(credentials) is None
        # Cleanup - none

    def test_validation_of_type_with_tuple_with_one_wrong_credential(self):
        # Setup
        credentials = (12345, "Password")
        # Exercise
        # Verify
        with pytest.raises(helpers.InvalidOnyxCredentialTypeError):
            helpers.validate_credentials_type(credentials)
        # Cleanup - none

    def test_validation_of_type_with_tuple_with_both_credentials_wrong(self):
        # Setup
        credentials = (12345, 6789)
        # Exercise
        # Verify
        with pytest.raises(helpers.InvalidOnyxCredentialTypeError):
            helpers.validate_credentials_type(credentials)
        # Cleanup - none


class TestValidateCredentialsPresence:
    def test_complete_set_of_credentials_scenario(self):
        # Setup
        credentials = ("Username", "Password")
        # Exercise
        # Verify
        assert helpers.validate_credentials_presence(credentials) is None
        # Cleanup - none

    def test_missing_username_scenario(self):
        # Setup
        credentials = (None, "Password")
        # Exercise
        # Verify
        with pytest.raises(helpers.MissingOnyxCredentialsError) as missing_credentials_error:
            helpers.validate_credentials_presence(credentials)
        assert str(missing_credentials_error.value) == "Missing username"
        # Cleanup - none

    def test_missing_password_scenario(self):
        # Setup
        credentials = ("Username", None)
        # Exercise
        # Verify
        with pytest.raises(helpers.MissingOnyxCredentialsError) as missing_credentials_error:
            helpers.validate_credentials_presence(credentials)
        assert str(missing_credentials_error.value) == "Missing password"
        # Cleanup - none

    def test_missing_credentials_scenario(self):
        # Setup
        credentials = (None, None)
        # Exercise
        # Verify
        with pytest.raises(helpers.MissingOnyxCredentialsError) as missing_credentials_error:
            helpers.validate_credentials_presence(credentials)
        assert str(missing_credentials_error.value) == "Missing username and password"
        # Cleanup - none


class TestValidateCredentials:
    def test_complete_set_of_correct_credentials_scenario(self):
        # Setup
        credentials = ("Username", "Password")
        # Exercise
        # Verify
        assert helpers.validate_credentials(credentials) is None
        # Cleanup - none

    def test_complete_set_of_credentials_with_wrong_username_type_scenario(self):
        # Setup
        credentials = (12345, "Password")
        # Exercise
        # Verify
        with pytest.raises(helpers.InvalidOnyxCredentialTypeError):
            helpers.validate_credentials(credentials)
        # Cleanup - none

    def test_complete_set_of_credentials_with_wrong_password_type_scenario(self):
        # Setup
        credentials = ("Username", 6789)
        # Exercise
        # Verify
        with pytest.raises(helpers.InvalidOnyxCredentialTypeError):
            helpers.validate_credentials(credentials)
        # Cleanup - none

    def test_missing_username_scenario(self):
        # Setup
        credentials = (None, "Password")
        # Exercise
        # Verify
        with pytest.raises(helpers.MissingOnyxCredentialsError) as missing_credentials_error:
            helpers.validate_credentials(credentials)
        assert str(missing_credentials_error.value) == "Missing username"
        # Cleanup - none

    def test_missing_password_scenario(self):
        # Setup
        credentials = ("Username", None)
        # Exercise
        # Verify
        with pytest.raises(helpers.MissingOnyxCredentialsError) as missing_credentials_error:
            helpers.validate_credentials(credentials)
        assert str(missing_credentials_error.value) == "Missing password"
        # Cleanup - none

    def test_missing_credentials_scenario(self):
        # Setup
        credentials = (None, None)
        # Exercise
        # Verify
        with pytest.raises(helpers.MissingOnyxCredentialsError) as missing_credentials_error:
            helpers.validate_credentials(credentials)
        assert str(missing_credentials_error.value) == "Missing username and password"
        # Cleanup - none


##########################################################################################

class TestCalculateNumberOfDiscoveredFiles:
    def test_count_of_discovered_files(
        self,
        mocked_files_available_to_download_multiple_sources_single_day,
    ):
        # Setup
        discovered_files = mocked_files_available_to_download_multiple_sources_single_day
        # Exercise
        file_count = helpers.calculate_number_of_discovered_files(discovered_files)
        # Verify
        expected_count = 6
        assert file_count == expected_count
        # Cleanup - none

    def test_count_of_discovered_files_with_no_file_discovered(self):
        # Setup
        discovered_files = []
        # Exercise
        file_count = helpers.calculate_number_of_discovered_files(discovered_files)
        # Verify
        expected_count = 0
        assert file_count == expected_count
        # Cleanup - none


class TestGenerateHumanReadableSize:
    @pytest.mark.parametrize(
        'byte_size, correct_human_readable_size', [
            (0, "0B"),
            (128, '128B'),
            (4754, '4.6 KiB'),
            (2097404, '2.0 MiB'),
            (7584723968, '7.1 GiB'),
            (2448999314432, '2.2 TiB')
        ]
    )
    def test_generation_of_human_readable_size(self, byte_size, correct_human_readable_size):
        # Setup
        # Exercise
        human_readable_size = helpers.generate_human_readable_size(byte_size)
        # Verify
        assert human_readable_size == correct_human_readable_size
        # Cleanup - none


class TestCalculateTotalDownloadSize:
    def test_total_download_size_calculation(
        self,
        mocked_files_available_to_download_multiple_sources_single_day,
    ):
        # Setup
        discovered_files = mocked_files_available_to_download_multiple_sources_single_day
        # Exercise
        total_download_size = helpers.calculate_total_download_size(discovered_files)
        # Verify
        expected_download_size = "167.5 MiB"
        assert total_download_size == expected_download_size
        # Cleanup - none

    def test_total_download_size_calculation_with_no_discovered_files(self):
        # Setup
        discovered_files = []
        # Exercise
        total_download_size = helpers.calculate_total_download_size(discovered_files)
        # Verify
        expected_download_size = "0B"
        assert total_download_size == expected_download_size
        # Cleanup - none
