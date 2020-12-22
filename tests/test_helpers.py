import pytest

import datavault_api_client.helpers as helpers


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
