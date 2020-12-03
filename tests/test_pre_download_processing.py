import pathlib

import pytest

from datavault_api_client import pre_download_processing as pdp
from datavault_api_client.data_structures import DownloadDetails


class TestGenerateFilePathMatchingDatavaultStructure:
    def test_file_path_generation(self):
        # Setup
        file_download_url = (
            "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S905/WATCHLIST/"
            "20200722-S905_WATCHLIST_username_0_0"
        )
        path_to_data_folder = pathlib.Path(__file__).resolve().parent.joinpath("Data").as_posix()
        file_name = "WATCHLIST_905_20200722.txt.bz2"
        # Exercise
        generated_file_path = pdp.generate_file_path_matching_datavault_structure(
            path_to_data_folder,
            file_name,
            file_download_url,
        )
        # Verify
        expected_file_path = pathlib.Path(__file__).resolve().parent.joinpath(
            "Data", "2020", "07", "22", "S905", "WATCHLIST", "WATCHLIST_905_20200722.txt.bz2"
        )
        assert generated_file_path == expected_file_path
        # Cleanup - none


class TestConvertMbToBytes:
    @pytest.mark.parametrize(
        "size_in_mib, correct_size_in_bytes",
        [(5.0, 5242880), (5.3, 5557453), (10.0, 10485760), (13.2, 13841203)],
    )
    def test_conversion_of_mb_in_bytes(self, size_in_mib, correct_size_in_bytes):
        # Setup - none
        # Exercise
        calculated_size_in_bytes = pdp.convert_mib_to_bytes(size_in_mib)
        # Verify
        assert calculated_size_in_bytes == correct_size_in_bytes
        # Cleanup - none


class TestCalculateMultiPartThreshold:
    @pytest.mark.parametrize(
        "partition_size_in_mib, correct_multi_part_threshold",
        [(5.0, 14680064), (5.3, 15560868), (10.0, 29360128), (13.2, 38755368)],
    )
    def test_calculation_of_multi_part_threshold(
        self, partition_size_in_mib, correct_multi_part_threshold
    ):
        # Setup - none
        # Exercise
        calculated_multi_part_threshold = pdp.calculate_multi_part_threshold(partition_size_in_mib)
        # Verify
        assert calculated_multi_part_threshold == correct_multi_part_threshold
        # Cleanup - none


class TestCheckIfPartitioned:
    @pytest.mark.parametrize(
        "file_size, partition_size_in_mib, expected_flag",
        [
            (23383245, 5.0, True),
            (12897485, 5.0, False),
            (48024781, 13.2, True),
            (34183577, 13.2, False),
        ],
    )
    def test_identification_of_files_to_split(
        self, file_size, partition_size_in_mib, expected_flag,
    ):
        # Setup - none
        # Exercise
        calculated_partitioned_flag = pdp.check_if_partitioned(
            file_size, partition_size_in_mib
        )
        # Verify
        assert calculated_partitioned_flag == expected_flag
        # Cleanup - none


class TestProcessRawDownloadInfo:
    def test_processing_of_raw_download_info(
        self,
        mocked_set_of_files_available_to_download_single_instrument
    ):
        # Setup
        path_to_data_folder = pathlib.Path(__file__).resolve().parent / "Data"
        size_of_partition_in_mib = 5.0
        # Exercise
        for file_details in mocked_set_of_files_available_to_download_single_instrument:
            processed_download_details = pdp.process_raw_download_info(
                file_details, path_to_data_folder, size_of_partition_in_mib
            )
        # Verify
        correct_download_details = DownloadDetails(
            file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0"
            ),
            file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                "Data", "2020", "07", "16", "S367", "WATCHLIST", "WATCHLIST_367_20200716.txt.bz2"
            ),
            size=100145874,
            md5sum="fb34325ec9262adc74c945a9e7c9b465",
            is_partitioned=True,
        )
        # noinspection PyUnboundLocalVariable
        assert processed_download_details == correct_download_details
        # Cleanup - none


class TestProcessAllDiscoveredFilesInfo:
    def test_processing_of_all_discovered_files_info(
        self,
        mocked_set_of_files_available_to_download_single_source_single_day,
        mocked_list_of_whole_files_download_details_single_source_single_day,
    ):
        # Setup
        path_to_data_folder = pathlib.Path(__file__).resolve().parent / "Data"
        partition_size_in_mb = 5.0
        # Exercise
        list_of_processed_download_details = pdp.process_all_discovered_files_info(
            mocked_set_of_files_available_to_download_single_source_single_day,
            path_to_data_folder,
            partition_size_in_mb,
        )
        # Verify
        assert (
            list_of_processed_download_details
            == mocked_list_of_whole_files_download_details_single_source_single_day
        )
        # Cleanup - none


class TestCalculateNumberOfSameSizePartitions:
    @pytest.mark.parametrize(
        "file_byte_size, partition_size_in_mb, correct_number_of_same_size_partitions",
        [(57671680, 5.0, 11), (100145874, 4.2, 22)],
    )
    def test_calculation_of_number_of_same_size_partitions(
        self,
        file_byte_size,
        partition_size_in_mb,
        correct_number_of_same_size_partitions,
    ):
        # Setup - none
        # Exercise
        calculated_number_of_same_size_partitions = pdp.calculate_number_of_same_size_partitions(
            file_byte_size, partition_size_in_mb
        )
        # Verify
        assert (
            calculated_number_of_same_size_partitions
            == correct_number_of_same_size_partitions
        )
        # Cleanup - none
