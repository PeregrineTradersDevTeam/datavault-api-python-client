import os
import pathlib
import datetime
import pytest

from datavault_api_client import post_download_processing as pdp
from datavault_api_client.data_structures import DownloadDetails, PartitionDownloadDetails


class TestGetNonPartitionedFiles:
    def test_identification_of_non_partitioned_files(
        self,
        mocked_list_of_whole_files_download_details_single_source_single_day,
    ):
        # Setup
        # Exercise
        non_partitioned_files = pdp.get_non_partitioned_files(
            mocked_list_of_whole_files_download_details_single_source_single_day,
        )
        # Verify
        expected_files = [
            DownloadDetails(
                file_name="COREREF_945_20200722.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/CORE/"
                    "20200722-S945_CORE_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data/2020/07/22/S945/CORE", "COREREF_945_20200722.txt.bz2"
                ),
                source_id=945,
                reference_date=datetime.datetime(year=2020, month=7, day=22),
                size=17734,
                md5sum="3548e03c8833b0e2133c80ac3b1dcdac",
                is_partitioned=False,
            ),
            DownloadDetails(
                file_name="CROSSREF_945_20200722.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/CROSS/"
                    "20200722-S945_CROSS_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data/2020/07/22/S945/CROSS", "CROSSREF_945_20200722.txt.bz2"
                ),
                source_id=945,
                reference_date=datetime.datetime(year=2020, month=7, day=22),
                size=32822,
                md5sum="936c0515dcbc27d2e2fc3ebdcf5f883a",
                is_partitioned=False,
            ),
        ]
        assert non_partitioned_files == expected_files
        # Cleanup - none

    def test_scenario_without_non_partitioned_files(
        self,
        mocked_list_of_whole_files_download_details_single_source_single_day,
    ):
        # Setup
        files_to_filter = [
            file for file in mocked_list_of_whole_files_download_details_single_source_single_day
            if file.is_partitioned is True
        ]
        # Exercise
        non_partitioned_files = pdp.get_non_partitioned_files(files_to_filter)
        # Verify
        assert non_partitioned_files == []
        # Cleanup - none


class TestGetPartitionedFiles:
    def test_identification_of_partitioned_files(
        self,
        mocked_list_of_whole_files_download_details_single_source_single_day,
    ):
        # Setup
        # Exercise
        partitioned_files = pdp.get_partitioned_files(
            mocked_list_of_whole_files_download_details_single_source_single_day,
        )
        # Verify
        expected_files = [
            DownloadDetails(
                file_name="WATCHLIST_945_20200722.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                    "20200722-S945_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722.txt.bz2"
                ),
                source_id=945,
                reference_date=datetime.datetime(year=2020, month=7, day=22),
                size=61663360,
                md5sum="78571e930fb12fcfb2fb70feb07c7bcf",
                is_partitioned=True,
            ),
        ]
        assert partitioned_files == expected_files
        # Cleanup - none

    def test_scenario_with_no_partitioned_files(
        self,
        mocked_list_of_whole_files_download_details_single_source_single_day,
    ):
        # Setup
        files_to_filter = [
            file for file in mocked_list_of_whole_files_download_details_single_source_single_day
            if file.is_partitioned is False
        ]
        # Exercise
        partitioned_files = pdp.get_partitioned_files(files_to_filter)
        # Verify
        assert partitioned_files == []
        # Cleanup - none


class TestGetPartitionsDownloadDetails:
    def test_identification_of_partition_details(
        self,
        mocked_list_of_whole_files_and_partitions_download_details_single_source_single_day,
    ):
        # Setup
        files_and_partitions = (
            mocked_list_of_whole_files_and_partitions_download_details_single_source_single_day
        )
        # Exercise
        computed_partitions_download_details = pdp.get_partitions_download_details(
                files_and_partitions,
        )
        # Verify
        expected_partitions_download_details = [
            item for item in files_and_partitions
            if type(item) is PartitionDownloadDetails]
        assert computed_partitions_download_details == expected_partitions_download_details
        # Cleanup - none

    def test_identification_of_name_specific_partition_download_details(
        self,
        mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
    ):
        # Setup
        parent_file_name = 'WATCHLIST_367_20200721.txt.bz2'
        files_and_partitions = (
            mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day
        )
        # Exercise
        computed_list_of_name_specific_partitions = pdp.get_partitions_download_details(
                files_and_partitions, parent_file_name,
        )
        # Verify
        expected_names_of_specific_partitions = [
            item for item in files_and_partitions
            if type(item) is PartitionDownloadDetails and item.parent_file_name == parent_file_name
        ]
        assert computed_list_of_name_specific_partitions == expected_names_of_specific_partitions
        # Cleanup - none


class TestGetListOfDownloadedPartitions:
    def test_retrieval_of_downloaded_partitions(self, simulated_downloaded_partitions):
        # Setup
        # Exercise
        computed_list_of_downloaded_partitions = pdp.get_downloaded_partitions(
            simulated_downloaded_partitions,
        )
        # Verify
        path_to_tmp_dir = simulated_downloaded_partitions
        expected_list_of_downloaded_partitions = [
            path_to_tmp_dir / 'WATCHLIST_367_20200721_1.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_2.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_3.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_4.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_5.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_6.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_7.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_8.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_9.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_10.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_11.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_12.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_13.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_14.txt',
            path_to_tmp_dir / 'WATCHLIST_367_20200721_15.txt']
        assert computed_list_of_downloaded_partitions == expected_list_of_downloaded_partitions
        # Cleanup - none

    def test_empty_directory_scenario(self):
        # Setup
        directory = pathlib.Path(__file__).resolve().parent.joinpath(
            "static_data", "Temp"
        )
        directory.mkdir(exist_ok=True)
        # Exercise
        downloaded_partitions = pdp.get_downloaded_partitions(directory)
        # Verify
        assert downloaded_partitions == []
        # Cleanup
        directory.rmdir()


class TestGetFileSpecificMissingPartitions:
    def test_identification_of_missing_partitions(
        self,
        mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
    ):
        # Setup
        base_path = pathlib.Path(__file__).resolve().parent.joinpath(
            'Data', '2020', '07', '21', 'S367', 'WATCHLIST',
        )
        base_path.mkdir(exist_ok=True, parents=True)
        downloaded_partition_file_names = [
            'WATCHLIST_367_20200721_1.txt',
            'WATCHLIST_367_20200721_2.txt',
            'WATCHLIST_367_20200721_3.txt',
            'WATCHLIST_367_20200721_4.txt',
            'WATCHLIST_367_20200721_5.txt',
            'WATCHLIST_367_20200721_6.txt',
            'WATCHLIST_367_20200721_7.txt',
            'WATCHLIST_367_20200721_8.txt',
            'WATCHLIST_367_20200721_9.txt',
            'WATCHLIST_367_20200721_10.txt',
            'WATCHLIST_367_20200721_11.txt',
            'WATCHLIST_367_20200721_12.txt',
            'WATCHLIST_367_20200721_13.txt',
            'WATCHLIST_367_20200721_14.txt',
        ]
        for file in downloaded_partition_file_names:
            path_to_downloaded_partition = base_path / file
            path_to_downloaded_partition.touch()

        file_specific_download_details = DownloadDetails(
            file_name='WATCHLIST_367_20200721.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/'
                'WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
            ),
            file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                'Data', '2020', '07', '21', 'S367', 'WATCHLIST', 'WATCHLIST_367_20200721.txt.bz2',
            ),
            source_id=367,
            reference_date=datetime.datetime(year=2020, month=7, day=21),
            size=82451354,
            md5sum='62df718ef5eb5f9f1ea3f6ea1f826c30',
            is_partitioned=True,
        )
        # Exercise
        computed_missing_partitions = pdp.get_file_specific_missing_partitions(
            file_specific_download_details,
            mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
        )
        # Verify
        expected_missing_partitions = [
            PartitionDownloadDetails(
                parent_file_name='WATCHLIST_367_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/'
                    'S367/WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
                    '?start=73400321&end=78643200'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S367', 'WATCHLIST',
                    'WATCHLIST_367_20200721_15.txt',
                ),
                partition_index=15,
            ),
            PartitionDownloadDetails(
                parent_file_name='WATCHLIST_367_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/'
                    'S367/WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
                    '?start=78643201&end=82451354'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21',
                    'S367', 'WATCHLIST', 'WATCHLIST_367_20200721_16.txt',
                ),
                partition_index=16,
            )
        ]
        assert computed_missing_partitions == expected_missing_partitions
        # Cleanup
        # First, remove all the created files
        for file in list(base_path.glob('**/*.txt')):
            file.unlink()
        # Then remove all the created folders iteratively:
        directory_root = pathlib.Path(__file__).resolve().parent / 'Data'
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()

    def test_no_missing_partition_scenario(
        self,
        mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
    ):
        # Setup
        base_path = pathlib.Path(__file__).resolve().parent.joinpath(
            'Data', '2020', '07', '21', 'S367', 'WATCHLIST',
        )
        base_path.mkdir(exist_ok=True, parents=True)
        downloaded_partition_file_names = [
            'WATCHLIST_367_20200721_1.txt',
            'WATCHLIST_367_20200721_2.txt',
            'WATCHLIST_367_20200721_3.txt',
            'WATCHLIST_367_20200721_4.txt',
            'WATCHLIST_367_20200721_5.txt',
            'WATCHLIST_367_20200721_6.txt',
            'WATCHLIST_367_20200721_7.txt',
            'WATCHLIST_367_20200721_8.txt',
            'WATCHLIST_367_20200721_9.txt',
            'WATCHLIST_367_20200721_10.txt',
            'WATCHLIST_367_20200721_11.txt',
            'WATCHLIST_367_20200721_12.txt',
            'WATCHLIST_367_20200721_13.txt',
            'WATCHLIST_367_20200721_14.txt',
            'WATCHLIST_367_20200721_15.txt',
            'WATCHLIST_367_20200721_16.txt',
        ]
        for file in downloaded_partition_file_names:
            path_to_downloaded_partition = base_path / file
            path_to_downloaded_partition.touch()

        file_specific_download_details = DownloadDetails(
            file_name='WATCHLIST_367_20200721.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/'
                'WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
            ),
            file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                'Data', '2020', '07', '21', 'S367', 'WATCHLIST', 'WATCHLIST_367_20200721.txt.bz2',
            ),
            source_id=367,
            reference_date=datetime.datetime(year=2020, month=7, day=21),
            size=82451354,
            md5sum='62df718ef5eb5f9f1ea3f6ea1f826c30',
            is_partitioned=True,
        )
        # Exercise
        computed_missing_partitions = pdp.get_file_specific_missing_partitions(
            file_specific_download_details,
            mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
        )
        # Verify
        assert computed_missing_partitions == []
        # Cleanup
        # First, remove all the created files
        for file in list(base_path.glob('**/*.txt')):
            file.unlink()
        # Then remove all the created folders iteratively:
        directory_root = pathlib.Path(__file__).resolve().parent / 'Data'
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()


class TestGetAllMissingPartitions:
    def test_identification_of_missing_partitions(
        self,
        mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
        mocked_download_details_multiple_sources_single_day
    ):
        # Setup
        base_path = pathlib.Path(__file__).resolve().parent.joinpath(
            "Data", "2020", "07", "21",
        )
        downloaded_instruments_directories = [
            base_path / 'S207' / 'CROSS',
            base_path / 'S207' / 'WATCHLIST',
            base_path / 'S367' / 'WATCHLIST',
            ]
        for directory in downloaded_instruments_directories:
            directory.mkdir(parents=True, exist_ok=True)

        list_of_downloaded_partitions = [
            base_path / 'S207' / 'CROSS' / 'CROSSREF_207_20200721_1.txt',
            base_path / 'S207' / 'CROSS' / 'CROSSREF_207_20200721_2.txt',
            base_path / 'S207' / 'CROSS' / 'CROSSREF_207_20200721_3.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_2.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_3.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_4.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_5.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_6.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_7.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_8.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_9.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_10.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_12.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_13.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_14.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_1.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_2.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_3.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_4.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_5.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_6.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_7.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_8.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_9.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_10.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_11.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_12.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_13.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_14.txt',
            ]

        for partition_path in list_of_downloaded_partitions:
            partition_path.touch()

        # Exercise
        missing_partitions = pdp.get_all_missing_partitions(
                mocked_download_details_multiple_sources_single_day,
                mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
            )
        # Verify
        expected_missing_partitions = [
            PartitionDownloadDetails(
                parent_file_name='WATCHLIST_207_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/'
                    'WATCHLIST/20200721-S207_WATCHLIST_username_0_0?start=0&end=5242880'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S207', 'WATCHLIST', 'WATCHLIST_207_20200721_1.txt',
                ),
                partition_index=1,
            ),
            PartitionDownloadDetails(
                parent_file_name='WATCHLIST_207_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/'
                    'WATCHLIST/20200721-S207_WATCHLIST_username_0_0?start=52428801&end=57671680'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S207', 'WATCHLIST',
                    'WATCHLIST_207_20200721_11.txt',
                ),
                partition_index=11,
            ),
            PartitionDownloadDetails(
                parent_file_name='WATCHLIST_367_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/'
                    'S367/WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
                    '?start=73400321&end=78643200'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S367', 'WATCHLIST',
                    'WATCHLIST_367_20200721_15.txt',
                ),
                partition_index=15,
            ),
            PartitionDownloadDetails(
                parent_file_name='WATCHLIST_367_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/'
                    'S367/WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
                    '?start=78643201&end=82451354'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S367', 'WATCHLIST',
                    'WATCHLIST_367_20200721_16.txt',
                ),
                partition_index=16,
            )
        ]
        assert missing_partitions == expected_missing_partitions
        # Cleanup
        # First, remove all the created files
        for file in list(base_path.glob('**/*.txt')):
            file.unlink()
        # Then remove all the created folders iteratively:
        directory_root = pathlib.Path(__file__).resolve().parent / 'Data'
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()

    def test_scenario_of_one_file_only_with_missing_partitions(
        self,
        mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
        mocked_download_details_multiple_sources_single_day
    ):
        # Setup
        base_path = pathlib.Path(__file__).resolve().parent.joinpath(
            "Data", "2020", "07", "21",
        )
        downloaded_instruments_directories = [
            base_path / 'S207' / 'CROSS',
            base_path / 'S207' / 'WATCHLIST',
            base_path / 'S367' / 'WATCHLIST',
            ]
        for directory in downloaded_instruments_directories:
            directory.mkdir(parents=True, exist_ok=True)

        list_of_downloaded_partitions = [
            base_path / 'S207' / 'CROSS' / 'CROSSREF_207_20200721_1.txt',
            base_path / 'S207' / 'CROSS' / 'CROSSREF_207_20200721_2.txt',
            base_path / 'S207' / 'CROSS' / 'CROSSREF_207_20200721_3.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_1.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_2.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_3.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_4.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_5.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_6.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_7.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_8.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_9.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_10.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_11.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_12.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_13.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_14.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_1.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_2.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_3.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_4.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_5.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_6.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_7.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_8.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_9.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_10.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_11.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_12.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_13.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_14.txt',
            ]

        for partition_path in list_of_downloaded_partitions:
            partition_path.touch()

        # Exercise
        missing_partitions = pdp.get_all_missing_partitions(
                mocked_download_details_multiple_sources_single_day,
                mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
            )
        # Verify
        expected_missing_partitions = [
            PartitionDownloadDetails(
                parent_file_name='WATCHLIST_367_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/'
                    'S367/WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
                    '?start=73400321&end=78643200'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S367', 'WATCHLIST',
                    'WATCHLIST_367_20200721_15.txt',
                ),
                partition_index=15,
            ),
            PartitionDownloadDetails(
                parent_file_name='WATCHLIST_367_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/'
                    'S367/WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
                    '?start=78643201&end=82451354'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S367', 'WATCHLIST',
                    'WATCHLIST_367_20200721_16.txt',
                ),
                partition_index=16,
            )
        ]
        assert missing_partitions == expected_missing_partitions
        # Cleanup
        # First, remove all the created files
        for file in list(base_path.glob('**/*.txt')):
            file.unlink()
        # Then remove all the created folders iteratively:
        directory_root = pathlib.Path(__file__).resolve().parent / 'Data'
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()

    def test_scenario_of_no_missing_partitions(
        self,
        mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
        mocked_download_details_multiple_sources_single_day
    ):
        # Setup
        base_path = pathlib.Path(__file__).resolve().parent.joinpath(
            "Data", "2020", "07", "21",
        )
        downloaded_instruments_directories = [
            base_path / 'S207' / 'CROSS',
            base_path / 'S207' / 'WATCHLIST',
            base_path / 'S367' / 'WATCHLIST',
            ]
        for directory in downloaded_instruments_directories:
            directory.mkdir(parents=True, exist_ok=True)

        list_of_downloaded_partitions = [
            base_path / 'S207' / 'CROSS' / 'CROSSREF_207_20200721_1.txt',
            base_path / 'S207' / 'CROSS' / 'CROSSREF_207_20200721_2.txt',
            base_path / 'S207' / 'CROSS' / 'CROSSREF_207_20200721_3.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_1.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_2.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_3.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_4.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_5.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_6.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_7.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_8.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_9.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_10.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_11.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_12.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_13.txt',
            base_path / 'S207' / 'WATCHLIST' / 'WATCHLIST_207_20200721_14.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_1.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_2.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_3.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_4.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_5.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_6.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_7.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_8.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_9.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_10.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_11.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_12.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_13.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_14.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_15.txt',
            base_path / 'S367' / 'WATCHLIST' / 'WATCHLIST_367_20200721_16.txt',
            ]

        for partition_path in list_of_downloaded_partitions:
            partition_path.touch()

        # Exercise
        missing_partitions = pdp.get_all_missing_partitions(
                mocked_download_details_multiple_sources_single_day,
                mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
            )
        # Verify
        assert missing_partitions == []
        # Cleanup
        # First, remove all the created files
        for file in list(base_path.glob('**/*.txt')):
            file.unlink()
        # Then remove all the created folders iteratively:
        directory_root = pathlib.Path(__file__).resolve().parent / 'Data'
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()


class TestGetFilesWithMissingPartitions:
    def test_identification_of_files_with_missing_partitions(
        self,
        # mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
        mocked_download_details_multiple_sources_single_day
    ):
        # Setup
        missing_partitions = [
            PartitionDownloadDetails(
                parent_file_name='WATCHLIST_207_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/'
                    'WATCHLIST/20200721-S207_WATCHLIST_username_0_0?start=52428801&end=57671680'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S207', 'WATCHLIST',
                    'WATCHLIST_207_20200721_11.txt',
                ),
                partition_index=11,
            ),
            PartitionDownloadDetails(
                parent_file_name='WATCHLIST_367_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/'
                    'S367/WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
                    '?start=73400321&end=78643200'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S367', 'WATCHLIST',
                    'WATCHLIST_367_20200721_15.txt',
                ),
                partition_index=15,
            ),
            PartitionDownloadDetails(
                parent_file_name='WATCHLIST_367_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/'
                    'S367/WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
                    '?start=78643201&end=82451354'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S367', 'WATCHLIST',
                    'WATCHLIST_367_20200721_16.txt',
                ),
                partition_index=16,
            )
        ]
        # Exercise
        files_with_missing_partitions = pdp.get_files_with_missing_partitions(
                mocked_download_details_multiple_sources_single_day,
                missing_partitions,
        )
        # Verify
        expected_files_with_missing_partitions = [
            DownloadDetails(
                file_name='WATCHLIST_207_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/'
                    'WATCHLIST/20200721-S207_WATCHLIST_username_0_0'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S207', 'WATCHLIST',
                    'WATCHLIST_207_20200721.txt.bz2',
                ),
                source_id=207,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=72293374,
                md5sum='36e444a8362e7db52af50ee0f8dc0d2e',
                is_partitioned=True),
            DownloadDetails(
                file_name='WATCHLIST_367_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/'
                    'WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S367', 'WATCHLIST',
                    'WATCHLIST_367_20200721.txt.bz2'
                ),
                source_id=367,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=82451354,
                md5sum='62df718ef5eb5f9f1ea3f6ea1f826c30',
                is_partitioned=True),
        ]
        assert files_with_missing_partitions == expected_files_with_missing_partitions
        # Cleanup - none

    def test_scenario_of_single_file_with_missing_partitions(
        self,
        mocked_download_details_multiple_sources_single_day
    ):
        # Setup
        missing_partitions = [
            PartitionDownloadDetails(
                parent_file_name='WATCHLIST_207_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/'
                    'WATCHLIST/20200721-S207_WATCHLIST_username_0_0?start=52428801&end=57671680'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S207', 'WATCHLIST',
                    'WATCHLIST_207_20200721_11.txt',
                ),
                partition_index=11,
            ),
        ]
        # Exercise
        files_with_missing_partitions = pdp.get_files_with_missing_partitions(
                mocked_download_details_multiple_sources_single_day,
                missing_partitions,
        )
        # Verify
        expected_files_with_missing_partitions = [
            DownloadDetails(
                file_name='WATCHLIST_207_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/'
                    'WATCHLIST/20200721-S207_WATCHLIST_username_0_0'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S207', 'WATCHLIST',
                    'WATCHLIST_207_20200721.txt.bz2',
                ),
                source_id=207,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=72293374,
                md5sum='36e444a8362e7db52af50ee0f8dc0d2e',
                is_partitioned=True),
        ]
        assert files_with_missing_partitions == expected_files_with_missing_partitions
        # Cleanup - none

    def test_scenario_of_no_missing_partitions(
        self,
        mocked_download_details_multiple_sources_single_day
    ):
        # Setup
        missing_partitions = []
        # Exercise
        files_with_missing_partitions = pdp.get_files_with_missing_partitions(
                mocked_download_details_multiple_sources_single_day,
                missing_partitions,
        )
        # Verify
        assert files_with_missing_partitions == []
        # Cleanup - none


class TestGetFilesReadyForConcatenation:
    def test_identification_of_files_ready_for_concatenation(
        self,
        mocked_download_details_multiple_sources_single_day,
    ):
        # Setup
        whole_files_download_details = mocked_download_details_multiple_sources_single_day
        files_with_missing_partitions = [
            DownloadDetails(
                file_name="WATCHLIST_207_20200721.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/"
                    "WATCHLIST/20200721-S207_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data", "2020", "07", "21", "S207", "WATCHLIST",
                    "WATCHLIST_207_20200721.txt.bz2",
                ),
                source_id=207,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=72293374,
                md5sum="36e444a8362e7db52af50ee0f8dc0d2e",
                is_partitioned=True,
            )
        ]
        # Exercise
        computed_files_ready_for_concatenation = (
            pdp.get_files_ready_for_concatenation(
                whole_files_download_details,
                files_with_missing_partitions,
            )
        )
        # Verify
        expected_files_to_concatenate = [
            'CROSSREF_207_20200721.txt.bz2',
            'WATCHLIST_367_20200721.txt.bz2',
        ]
        expected_files_ready_for_concatenation = [
            item for item in whole_files_download_details
            if item.file_name in expected_files_to_concatenate
        ]
        assert computed_files_ready_for_concatenation == expected_files_ready_for_concatenation
        # Cleanup - none

    def test_no_file_ready_for_concatenation_scenario(
        self,
        mocked_download_details_multiple_sources_single_day,
    ):
        # Setup
        whole_files_download_details = mocked_download_details_multiple_sources_single_day
        files_with_missing_partitions = mocked_download_details_multiple_sources_single_day
        # Exercise
        files_ready_for_concatenation = (
            pdp.get_files_ready_for_concatenation(
                whole_files_download_details,
                files_with_missing_partitions,
            )
        )
        # Verify
        assert files_ready_for_concatenation == []
        # Cleanup - none


class TestConcatenatePartitions:
    def test_concatenation_of_files(self):
        # Setup
        base_path = pathlib.Path(__file__).resolve().parent.joinpath(
            'Temp', '2020', '07', '21', 'CROSS',
        )
        base_path.mkdir(parents=True, exist_ok=True)
        file_names = [
            'CROSSREF_207_20200721_1.txt',
            'CROSSREF_207_20200721_2.txt',
            'CROSSREF_207_20200721_3.txt',
        ]

        concatenated_content = b''

        for file_name in file_names:
            fpath = base_path / file_name
            with fpath.open('wb') as outfile:
                random_byte_content = os.urandom(500)
                concatenated_content += random_byte_content
                outfile.write(random_byte_content)

        path_to_output_file = base_path / 'CROSSREF_207_20200721.txt.bz2'
        # Execute
        concatenated_file = pdp.concatenate_partitions(
            path_to_output_file)
        # Verify
        assert concatenated_file == path_to_output_file.as_posix()

        with path_to_output_file.open('rb') as infile:
            file_content = infile.read()

        assert file_content == concatenated_content
        # Cleanup
        # First, remove all the created files
        for file in list(base_path.glob('**/*.bz2')):
            file.unlink()
        # Then remove all the created folders iteratively:
        directory_root = pathlib.Path(__file__).resolve().parent / 'Temp'
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()


class TestConcatenateEachFilePartitions:
    def test_concatenation_of_all_partitions(self):
        # Setup
        base_path = pathlib.Path(__file__).resolve().parent.joinpath(
            'Data', '2020', '07', '21', 'S207',
        )
        base_path.mkdir(parents=True, exist_ok=True)
        files_to_concatenate = [
            DownloadDetails(
                file_name='CROSSREF_207_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/'
                    'CROSS/20200721-S207_CROSS_ALL_0_0'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S207', 'CROSS',
                    'CROSSREF_207_20200721.txt.bz2',
                ),
                source_id=207,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=14690557,
                md5sum='f2683cd87a7b29f3b8776373d56a8456',
                is_partitioned=True,
            ),
            DownloadDetails(
                file_name='WATCHLIST_207_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/'
                    'WATCHLIST/20200721-S207_WATCHLIST_username_0_0'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '21', 'S207', 'WATCHLIST',
                    'WATCHLIST_207_20200721.txt.bz2',
                ),
                source_id=207,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=72293374,
                md5sum='36e444a8362e7db52af50ee0f8dc0d2e',
                is_partitioned=True,
            )
        ]
        partitions = {
            'CROSSREF_207_20200721.txt.bz2': [
                'CROSSREF_207_20200721_1.txt',
                'CROSSREF_207_20200721_2.txt',
                'CROSSREF_207_20200721_3.txt',
            ],
            'WATCHLIST_207_20200721.txt.bz2': [
                'WATCHLIST_207_20200721_1.txt',
                'WATCHLIST_207_20200721_2.txt',
                'WATCHLIST_207_20200721_3.txt',
                'WATCHLIST_207_20200721_4.txt',
                'WATCHLIST_207_20200721_5.txt',
                'WATCHLIST_207_20200721_6.txt',
                'WATCHLIST_207_20200721_7.txt',
                'WATCHLIST_207_20200721_8.txt',
                'WATCHLIST_207_20200721_9.txt',
                'WATCHLIST_207_20200721_10.txt',
                'WATCHLIST_207_20200721_11.txt',
                'WATCHLIST_207_20200721_12.txt',
                'WATCHLIST_207_20200721_13.txt',
                'WATCHLIST_207_20200721_14.txt',
            ],
        }

        crossref_concatenated_content = b''
        path_to_crossref_files_directory = base_path / 'CROSS'
        path_to_crossref_files_directory.mkdir(parents=True, exist_ok=True)
        for crossref_partition in partitions['CROSSREF_207_20200721.txt.bz2']:
            fpath = base_path / 'CROSS' / crossref_partition
            with fpath.open('wb') as outfile:
                random_byte_content = os.urandom(500)
                crossref_concatenated_content += random_byte_content
                outfile.write(random_byte_content)

        watchlist_concatenated_content = b''
        path_to_watchlist_files_directory = base_path / 'WATCHLIST'
        path_to_watchlist_files_directory.mkdir(parents=True, exist_ok=True)
        for watchlist_partition in partitions['WATCHLIST_207_20200721.txt.bz2']:
            fpath = base_path / 'WATCHLIST' / watchlist_partition
            with fpath.open('wb') as outfile:
                random_byte_content = os.urandom(500)
                watchlist_concatenated_content += random_byte_content
                outfile.write(random_byte_content)

        # Exercise
        files_to_test = pdp.concatenate_each_file_partitions(files_to_concatenate)
        # Verify
        path_to_crossref_file = pathlib.Path(__file__).resolve().parent.joinpath(
            'Data', '2020', '07', '21', 'S207', 'CROSS', 'CROSSREF_207_20200721.txt.bz2',
        )
        with path_to_crossref_file.open('rb') as infile:
            crossref_file_content = infile.read()

        assert crossref_file_content == crossref_concatenated_content

        path_to_watchlist_file = pathlib.Path(__file__).resolve().parent.joinpath(
            'Data', '2020', '07', '21', 'S207', 'WATCHLIST',
            'WATCHLIST_207_20200721.txt.bz2',
        )
        with path_to_watchlist_file.open('rb') as infile:
            watchlist_file_content = infile.read()

        assert watchlist_file_content == watchlist_concatenated_content

        assert files_to_test == files_to_concatenate
        # Cleanup
        # First, remove all the created files
        for file in list(base_path.glob('**/*.bz2')):
            file.unlink()
        # Then remove all the created folders iteratively:
        directory_root = pathlib.Path(__file__).resolve().parent / 'Data'
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()
