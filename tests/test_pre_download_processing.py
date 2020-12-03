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
        "file_size_in_bytes, partition_size_in_mib, correct_number_of_same_size_partitions",
        [(57671680, 5.0, 11), (100145874, 4.2, 22)],
    )
    def test_calculation_of_number_of_same_size_partitions(
        self,
        file_size_in_bytes,
        partition_size_in_mib,
        correct_number_of_same_size_partitions,
    ):
        # Setup - none
        # Exercise
        calculated_number_of_same_size_partitions = pdp.calculate_number_of_same_size_partitions(
            file_size_in_bytes, partition_size_in_mib
        )
        # Verify
        assert (
            calculated_number_of_same_size_partitions
            == correct_number_of_same_size_partitions
        )
        # Cleanup - none


class TestCalculateSizeOfLastPartition:
    @pytest.mark.parametrize(
        "file_size_in_bytes, partition_size_in_mib, correct_size_of_last_partition",
        [(57671680, 5.0, 0), (100145874, 4.7, 1579734)],
    )
    def test_calculation_size_of_last_partition(
        self, file_size_in_bytes, partition_size_in_mib, correct_size_of_last_partition
    ):
        # Setup - none
        # Exercise
        calculated_size_of_last_partition = pdp.calculate_size_of_last_partition(
            file_size_in_bytes, partition_size_in_mib,
        )
        # Verify
        assert calculated_size_of_last_partition == correct_size_of_last_partition
        # Cleanup - none


class TestCalculateListOfPartitionUpperExtremities:
    def test_calculation_of_upper_extremities(self):
        # Setup
        file_size = 61663360
        partition_size_in_mib = 5.0
        # Exercise
        calculated_list_of_upper_extremities = pdp.calculate_list_of_partition_upper_extremities(
            file_size, partition_size_in_mib
        )
        # Verify
        correct_list_of_upper_extremities = [
            5242880,
            10485760,
            15728640,
            20971520,
            26214400,
            31457280,
            36700160,
            41943040,
            47185920,
            52428800,
            57671680,
            61663360,
        ]
        assert calculated_list_of_upper_extremities == correct_list_of_upper_extremities
        # Cleanup - none

    def test_calculation_of_upper_extremities_when_file_size_is_multiple_of_partition_size(
        self,
    ):
        # Setup
        file_size = 57671680
        partition_size_in_mib = 5.0
        # Exercise
        calculated_list_of_upper_extremities = pdp.calculate_list_of_partition_upper_extremities(
            file_size, partition_size_in_mib
        )
        # Verify
        correct_list_of_upper_extremities = [
            5242880,
            10485760,
            15728640,
            20971520,
            26214400,
            31457280,
            36700160,
            41943040,
            47185920,
            52428800,
            57671680,
        ]
        assert calculated_list_of_upper_extremities == correct_list_of_upper_extremities
        # Cleanup - none


class TestCalculateListOfPartitionLowerExtremities:
    def test_calculation_of_lower_extremities(self):
        # Setup
        file_size = 61663360
        partition_size_in_mib = 5.0
        # Exercise
        calculated_list_of_upper_extremities = pdp.calculate_list_of_partition_lower_extremities(
            file_size, partition_size_in_mib
        )
        # Verify
        correct_list_of_upper_extremities = [
            0,
            5242881,
            10485761,
            15728641,
            20971521,
            26214401,
            31457281,
            36700161,
            41943041,
            47185921,
            52428801,
            57671681,
        ]
        assert calculated_list_of_upper_extremities == correct_list_of_upper_extremities
        # Cleanup - none

    def test_calculation_of_lower_extremities_when_file_size_is_multiple_of_partition_size(
        self,
    ):
        # Setup
        file_size = 57671680
        partition_size_in_mib = 5.0
        # Exercise
        calculated_list_of_upper_extremities = pdp.calculate_list_of_partition_lower_extremities(
            file_size, partition_size_in_mib
        )
        # Verify
        correct_list_of_upper_extremities = [
            0,
            5242881,
            10485761,
            15728641,
            20971521,
            26214401,
            31457281,
            36700161,
            41943041,
            47185921,
            52428801,
        ]
        assert calculated_list_of_upper_extremities == correct_list_of_upper_extremities
        # Cleanup - none


class TestCalculateListOfPartitionExtremities:
    def test_calculation_of_partition_extremities(self):
        # Setup
        file_size = 61663360
        size_of_partition_in_mib = 5.0
        # Exercise
        calculated_partition_extremities = pdp.calculate_list_of_partition_extremities(
            file_size, size_of_partition_in_mib
        )
        # Verify
        correct_partition_extremities = [
            {"start": 0, "end": 5242880},
            {"start": 5242881, "end": 10485760},
            {"start": 10485761, "end": 15728640},
            {"start": 15728641, "end": 20971520},
            {"start": 20971521, "end": 26214400},
            {"start": 26214401, "end": 31457280},
            {"start": 31457281, "end": 36700160},
            {"start": 36700161, "end": 41943040},
            {"start": 41943041, "end": 47185920},
            {"start": 47185921, "end": 52428800},
            {"start": 52428801, "end": 57671680},
            {"start": 57671681, "end": 61663360},
        ]
        assert calculated_partition_extremities == correct_partition_extremities
        # Cleanup - none

    def test_calculation_of_partition_extremities_file_size_multiple_of_partition_size(
        self,
    ):
        # Setup
        file_size = 57671680
        size_of_partition_in_mib = 5.0
        # Exercise
        calculated_partition_extremities = pdp.calculate_list_of_partition_extremities(
            file_size, size_of_partition_in_mib
        )
        # Verify
        correct_partition_extremities = [
            {"start": 0, "end": 5242880},
            {"start": 5242881, "end": 10485760},
            {"start": 10485761, "end": 15728640},
            {"start": 15728641, "end": 20971520},
            {"start": 20971521, "end": 26214400},
            {"start": 26214401, "end": 31457280},
            {"start": 31457281, "end": 36700160},
            {"start": 36700161, "end": 41943040},
            {"start": 41943041, "end": 47185920},
            {"start": 47185921, "end": 52428800},
            {"start": 52428801, "end": 57671680},
        ]
        assert calculated_partition_extremities == correct_partition_extremities
        # Cleanup - none


class TestFormatQueryString:
    @pytest.mark.parametrize(
        "parameters_to_encode, correct_query_string",
        [
            ({"start": 0, "end": 943718}, "start=0&end=943718"),
            ({"start": 9437181, "end": 10380898}, "start=9437181&end=10380898"),
            ({"start": 24536669, "end": 25217299}, "start=24536669&end=25217299"),
        ],
    )
    def test_generation_of_query_string(
        self, parameters_to_encode, correct_query_string
    ):
        # Setup - none
        # Exercise
        generated_query_string = pdp.format_query_string(
            parameters_to_encode
        )
        # Verify
        assert generated_query_string == correct_query_string
        # Cleanup - none


class TestJoinBaseUrlAndQueryString:
    @pytest.mark.parametrize(
        "query_string, correct_encoded_url",
        [
            (
                "start=0&end=943718",
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
                "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0?start=0&end=943718",
            ),
            (
                "start=9437181&end=10380898",
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
                "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0?start=9437181&end=10380898",
            ),
            (
                "start=24536669&end=25217299",
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
                "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0?start=24536669&end=25217299",
            ),
        ],
    )
    def test_generation_of_url_with_query_string(
        self, query_string, correct_encoded_url
    ):
        # Setup
        base_url = (
            "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
            "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0"
        )
        # Exercise
        generated_url = pdp.join_base_url_and_query_string(
            base_url, query_string
        )
        # Verify
        assert generated_url == correct_encoded_url
        # Cleanup - none


class TestGeneratePartitionDownloadUrl:
    @pytest.mark.parametrize(
        "partition_extremities, correct_partition_download_url",
        [
            (
                {"start": 0, "end": 943718}, (
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
                    "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0?start=0&end=943718"
                ),
            ),
            (
                {"start": 9437181, "end": 10380898}, (
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
                    "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0?start=9437181&end=10380898"
                ),
            ),
            (
                {"start": 24536669, "end": 25217299}, (
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
                    "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0"
                    "?start=24536669&end=25217299"
                ),
            ),
        ],
    )
    def test_generation_of_partition_download_url(
        self, partition_extremities, correct_partition_download_url
    ):
        # Setup
        whole_file_download_url = (
            "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
            "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0"
        )
        # Exercise
        generated_partition_download_url = pdp.create_partition_download_url(
            whole_file_download_url, partition_extremities
        )
        # Verify
        assert generated_partition_download_url == correct_partition_download_url
        # Cleanup - none

    def test_generation_of_partition_url_when_base_url_has_trailing_slash(self):
        # Setup
        whole_file_download_url = (
            "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
            "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0/"
        )
        partition_extremities = {"start": 24536669, "end": 25217299}
        # Exercise
        generated_partition_download_url = pdp.create_partition_download_url(
            whole_file_download_url, partition_extremities
        )
        # Verify
        correct_partition_download_url = (
            "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
            "S905/WATCHLIST/"
            "20200722-S905_WATCHLIST_username_0_0?start=24536669&end=25217299"
        )
        assert generated_partition_download_url == correct_partition_download_url
        # Cleanup - none


class TestGeneratePathToFilePartition:
    @pytest.mark.parametrize(
        "partition_index, correct_path_to_file_partition",
        [
            (
                0,
                pathlib.Path(__file__).resolve().parent
                / "Data"
                / "2020"
                / "07"
                / "22"
                / "S905"
                / "WATCHLIST"
                / "WATCHLIST_905_20200722_1.txt",
            ),
            (
                1,
                pathlib.Path(__file__).resolve().parent
                / "Data"
                / "2020"
                / "07"
                / "22"
                / "S905"
                / "WATCHLIST"
                / "WATCHLIST_905_20200722_2.txt",
            ),
            (
                32,
                pathlib.Path(__file__).resolve().parent
                / "Data"
                / "2020"
                / "07"
                / "22"
                / "S905"
                / "WATCHLIST"
                / "WATCHLIST_905_20200722_33.txt",
            ),
        ],
    )
    def test_generation_of_path_to_file_partition(
        self, partition_index, correct_path_to_file_partition
    ):
        # Setup
        path_to_file = (
            pathlib.Path(__file__).resolve().parent
            / "Data"
            / "2020"
            / "07"
            / "22"
            / "S905"
            / "WATCHLIST"
            / "WATCHLIST_905_20200722.txt.bz2"
        )
        # Exercise
        generated_path_to_file_partition = pdp.generate_path_to_file_partition(
            path_to_file, partition_index
        )
        # Verify
        assert generated_path_to_file_partition == correct_path_to_file_partition
        # Cleanup - none


class TestCreateListOfFilePartitionsDownloadInfo:
    def test_generation_list_of_partitions_download_info(
        self,
        mocked_download_details_single_instrument,
        mocked_list_of_file_partitions_single_instrument,
    ):
        # Setup
        file_specific_download_details = mocked_download_details_single_instrument
        partition_size_in_mb = 5.0
        # Exercise
        partitions_download_info = pdp.create_list_of_file_specific_partition_download_info(
            file_specific_download_details, partition_size_in_mb
        )
        # Verify
        assert partitions_download_info == mocked_list_of_file_partitions_single_instrument
        # Cleanup - none
