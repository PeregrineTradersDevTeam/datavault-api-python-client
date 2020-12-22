import datetime
import os
import pathlib

from datavault_api_client import post_download_processing as pdp
from datavault_api_client.data_structures import (
    ConcurrentDownloadManifest,
    DownloadDetails,
    PartitionDownloadDetails,
)


class TestGetNonPartitionedFiles:
    def test_identification_of_non_partitioned_files(
        self,
        mocked_whole_files_download_details_single_source_single_day,
    ):
        # Setup
        # Exercise
        non_partitioned_files = pdp.get_non_partitioned_files(
            mocked_whole_files_download_details_single_source_single_day,
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
        mocked_whole_files_download_details_single_source_single_day,
    ):
        # Setup
        files_to_filter = [
            file for file in mocked_whole_files_download_details_single_source_single_day
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
        mocked_whole_files_download_details_single_source_single_day,
    ):
        # Setup
        # Exercise
        partitioned_files = pdp.get_partitioned_files(
            mocked_whole_files_download_details_single_source_single_day,
        )
        # Verify
        expected_files = [
            DownloadDetails(
                file_name="WATCHLIST_945_20200722.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/"
                    "WATCHLIST/20200722-S945_WATCHLIST_username_0_0"
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
        mocked_whole_files_download_details_single_source_single_day,
    ):
        # Setup
        files_to_filter = [
            file for file in mocked_whole_files_download_details_single_source_single_day
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
        mocked_partitions_download_details_single_source_single_day,
    ):
        # Setup
        partitions = mocked_partitions_download_details_single_source_single_day
        # Exercise
        computed_partitions_download_details = pdp.get_partitions_download_details(
            partitions,
        )
        # Verify
        assert computed_partitions_download_details == partitions
        # Cleanup - none

    def test_identification_of_name_specific_partition_download_details(
        self,
        mocked_partitions_download_details_multiple_sources_single_day,
    ):
        # Setup
        parent_file_name = 'WATCHLIST_367_20200721.txt.bz2'
        partitions = mocked_partitions_download_details_multiple_sources_single_day
        # Exercise
        computed_list_of_name_specific_partitions = pdp.get_partitions_download_details(
            partitions,
            parent_file_name,
        )
        # Verify
        expected_names_of_specific_partitions = [
            item for item in partitions
            if item.parent_file_name == parent_file_name
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
        mocked_partitions_download_details_multiple_sources_single_day,
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
            mocked_partitions_download_details_multiple_sources_single_day,
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
        mocked_partitions_download_details_multiple_sources_single_day,
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
            mocked_partitions_download_details_multiple_sources_single_day,
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
        mocked_partitions_download_details_multiple_sources_single_day,
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
                mocked_partitions_download_details_multiple_sources_single_day,
            )
        missing_partitions.sort(key=lambda x: x.parent_file_name)
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
        mocked_partitions_download_details_multiple_sources_single_day,
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
                mocked_partitions_download_details_multiple_sources_single_day,
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
        mocked_partitions_download_details_multiple_sources_single_day,
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
                mocked_partitions_download_details_multiple_sources_single_day,
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
        files_ready_for_concatenation = (
            pdp.get_files_ready_for_concatenation(
                whole_files_download_details,
                files_with_missing_partitions,
            )
        )
        files_ready_for_concatenation.sort(key=lambda x: x.file_name)
        # Verify
        expected_files_to_concatenate = [
            'CROSSREF_207_20200721.txt.bz2',
            'WATCHLIST_367_20200721.txt.bz2',
        ]
        expected_files_ready_for_concatenation = [
            item for item in whole_files_download_details
            if item.file_name in expected_files_to_concatenate
        ]
        assert files_ready_for_concatenation == expected_files_ready_for_concatenation
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


class TestFilterFilesReadyForIntegrityTest:
    def test_no_file_with_missing_partitions_scenario(
        self,
        mocked_download_details_multiple_sources_single_day,
    ):
        # Setup
        whole_files_download_manifest = mocked_download_details_multiple_sources_single_day
        files_with_missing_partitions = []
        # Exercise
        files_ready_for_integrity_test = pdp.get_files_ready_for_integrity_test(
            files_with_missing_partitions,
            whole_files_download_manifest,
        )
        # Verify
        assert (
            files_ready_for_integrity_test.sort(key=lambda x: x.file_name) ==
            whole_files_download_manifest.sort(key=lambda x: x.file_name)
        )
        # Cleanup - none

    def test_files_with_missing_partitions_scenario(
        self,
        mocked_download_details_multiple_sources_single_day,
    ):
        # Setup
        whole_files_download_manifest = mocked_download_details_multiple_sources_single_day
        files_with_missing_partitions = [
            DownloadDetails(
                file_name="WATCHLIST_207_20200721.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/"
                    "WATCHLIST/20200721-S207_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721.txt.bz2"
                ),
                source_id=207,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=72293374,
                md5sum="36e444a8362e7db52af50ee0f8dc0d2e",
                is_partitioned=True,
            ),
            DownloadDetails(
                file_name="WATCHLIST_367_20200721.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/"
                    "WATCHLIST/20200721-S367_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721.txt.bz2"
                ),
                source_id=367,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=82451354,
                md5sum="62df718ef5eb5f9f1ea3f6ea1f826c30",
                is_partitioned=True,
            ),
        ]
        files_ready_for_integrity_test = pdp.get_files_ready_for_integrity_test(
            files_with_missing_partitions,
            whole_files_download_manifest,
        )
        # Verify
        expected_files = [
            DownloadDetails(
                file_name="COREREF_207_20200721.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/"
                    "S207/CORE/20200721-S207_CORE_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data/2020/07/21/S207/CORE", "COREREF_207_20200721.txt.bz2"
                ),
                source_id=207,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=4590454,
                md5sum="c1a079841f84676e91b5021afd3f5272",
                is_partitioned=False,
            ),
            DownloadDetails(
                file_name="COREREF_367_20200721.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/"
                    "S367/CORE/20200721-S367_CORE_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data/2020/07/21/S367/CORE", "COREREF_367_20200721.txt.bz2"
                ),
                source_id=367,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=706586,
                md5sum="e28385e918aa71720235232c9a895b64",
                is_partitioned=False,
            ),
            DownloadDetails(
                file_name="CROSSREF_207_20200721.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/CROSS/"
                    "20200721-S207_CROSS_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data/2020/07/21/S207/CROSS", "CROSSREF_207_20200721.txt.bz2"
                ),
                source_id=207,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=14690557,
                md5sum="f2683cd87a7b29f3b8776373d56a8456",
                is_partitioned=True,
            ),
            DownloadDetails(
                file_name="CROSSREF_367_20200721.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/CROSS/"
                    "20200721-S367_CROSS_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data/2020/07/21/S367/CROSS", "CROSSREF_367_20200721.txt.bz2"
                ),
                source_id=367,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=879897,
                md5sum="fdb7592c8806a28f59c4d4da1e934c43",
                is_partitioned=False,
            ),
        ]
        assert (
            files_ready_for_integrity_test.sort(key=lambda x: x.file_name) ==
            expected_files.sort(key=lambda x: x.file_name)
        )
        # Cleanup - none


class TestPreConcatenationProcessing:
    def test_scenario_1(
        self,
        mocked_concurrent_download_manifest,
    ):
        """Tests the pre_concatenation_processing function.

        Testing scenario:
            - Whole file correctly downloaded,
            - COREREF file correctly downloaded (all partitions are downloaded)
            - WATCHLIST file missing two partitions

        Expected output:
            A ConcurrentDownloadManifest with the whole_files_reference field consisting
            only of the WATCHLIST file and the concurrent_download_manifest field
            consisting of a list with the download details of the two missing partitions.
        """
        # Setup
        partitions_to_create = [
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_1.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_2.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_3.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_4.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_5.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_1.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_2.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_3.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_4.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_5.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_6.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_7.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_8.txt"
            ),
        ]

        for partition_path in partitions_to_create:
            partition_path.touch()
        # Exercise
        failed_downloads = pdp.pre_concatenation_processing(
            mocked_concurrent_download_manifest,
        )
        # Verify
        expected_failed_downloads = ConcurrentDownloadManifest(
            files_reference_data=[
                DownloadDetails(
                    file_name="WATCHLIST_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "WATCHLIST/20201218-S945_WATCHLIST_username_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/WATCHLIST/WATCHLIST_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=51648457,
                    md5sum="11c5253a7cd1743aea93ec5124fd974d",
                    is_partitioned=True,
                ),
            ],
            whole_files_to_download=[],
            partitions_to_download=[
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=41943041&end=47185920'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_9.txt"
                    ),
                    partition_index=9,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=47185921&end=51648457'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_10.txt"
                    ),
                    partition_index=10,
                )
            ]
        )
        assert failed_downloads == expected_failed_downloads
        # Cleanup - none
        for partition in partitions_to_create:
            partition.unlink()

    def test_scenario_2(
        self,
        mocked_concurrent_download_manifest,
    ):
        """Tests the pre_concatenation_processing function.

        Testing scenario:
            - Whole file correctly downloaded,
            - COREREF file correctly downloaded (all partitions are downloaded)
            - WATCHLIST file correctly downloaded

        Expected output:
            A ConcurrentDownloadManifest with the whole_files_reference field consisting
            of an empty list and the concurrent_download_manifest field consisting of an
            empty list as well.
        """
        # Setup
        partitions_to_create = [
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_1.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_2.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_3.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_4.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_5.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_1.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_2.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_3.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_4.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_5.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_6.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_7.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_8.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_9.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_10.txt"
            ),
        ]

        for partition_path in partitions_to_create:
            partition_path.touch()

        # Exercise
        failed_downloads = pdp.pre_concatenation_processing(
            mocked_concurrent_download_manifest,
        )
        # Verify
        expected_failed_downloads = ConcurrentDownloadManifest(
            files_reference_data=[],
            whole_files_to_download=[],
            partitions_to_download=[],
        )
        assert failed_downloads == expected_failed_downloads
        # Cleanup - none
        for partition in partitions_to_create:
            partition.unlink()

    def test_scenario_3(
        self,
        mocked_concurrent_download_manifest,
    ):
        """Tests the pre_concatenation_processing function.

        Testing scenario:
            - Whole file correctly downloaded,
            - COREREF file missing a partition
            - WATCHLIST file missing a partition

        Expected output:
            A ConcurrentDownloadManifest with the whole_files_reference field consisting
            of an empty list and the concurrent_download_manifest field consisting of an
            empty list as well.
        """
        # Setup
        partitions_to_create = [
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_1.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_2.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_3.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_5.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_1.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_2.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_3.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_4.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_5.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_6.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_7.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_8.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_10.txt"
            ),
        ]

        for partition_path in partitions_to_create:
            partition_path.touch()
        # Exercise
        failed_downloads = pdp.pre_concatenation_processing(
            mocked_concurrent_download_manifest,
        )
        # Verify
        expected_failed_downloads = ConcurrentDownloadManifest(
            files_reference_data=[
                DownloadDetails(
                    file_name="COREREF_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/"
                        "20201218-S945_CORE_ALL_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/CORE/COREREF_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=24326963,
                    md5sum="8fc8fa1402e23f2d552899525b808514",
                    is_partitioned=True,
                ),
                DownloadDetails(
                    file_name="WATCHLIST_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "WATCHLIST/20201218-S945_WATCHLIST_username_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/WATCHLIST/WATCHLIST_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=51648457,
                    md5sum="11c5253a7cd1743aea93ec5124fd974d",
                    is_partitioned=True,
                ),
            ],
            whole_files_to_download=[],
            partitions_to_download=[
                PartitionDownloadDetails(
                    parent_file_name='COREREF_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/'
                        '20201218-S945_CORE_ALL_0_0?start=15728641&end=20971520'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/CORE/"
                        "COREREF_945_20201218_4.txt"
                    ),
                    partition_index=4,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=41943041&end=47185920'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_9.txt"
                    ),
                    partition_index=9,
                ),
            ]
        )
        assert (failed_downloads.files_reference_data.sort(key=lambda x: x.file_name)
                == expected_failed_downloads.files_reference_data.sort(key=lambda x: x.file_name))
        assert (
            failed_downloads.partitions_to_download.sort(key=lambda x: x.parent_file_name)
            == expected_failed_downloads.partitions_to_download.sort(
                key=lambda x: x.parent_file_name)
        )
        # Cleanup - none
        for partition in partitions_to_create:
            partition.unlink()

    def test_scenario_4(
        self,
        mocked_concurrent_download_manifest,
    ):
        """Tests the pre_concatenation_processing function.

        Testing scenario:
            - CROSSREF file incorrectly downloaded,
            - COREREF file correctly downloaded (all partitions are downloaded)
            - WATCHLIST file correctly downloaded

        Expected output:
            A ConcurrentDownloadManifest with the whole_files_reference field consisting
            of an empty list and the concurrent_download_manifest field consisting of an
            empty list as well.
        """
        # Setup
        partitions_to_create = [
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_1.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_2.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_3.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_4.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/CORE/COREREF_945_20201218_5.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_1.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_2.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_3.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_4.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_5.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_6.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_7.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_8.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_9.txt"
            ),
            pathlib.Path(__file__).resolve().parent.joinpath(
                "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                "WATCHLIST_945_20201218_10.txt"
            ),
        ]

        for partition_path in partitions_to_create:
            partition_path.touch()

        crossref_file_path = pathlib.Path(__file__).resolve().parent.joinpath(
            "static_data/post_processing_scenario_1",
            "2020/12/18/CROSS/CROSSREF_945_20201218.txt.bz2",
        )
        crossref_destination = pathlib.Path(__file__).resolve().parent.joinpath(
            "static_data/CROSSREF_945_20201218.txt.bz2"
        )
        if not crossref_destination.exists():
            crossref_file_path.replace(crossref_destination)

        crossref_file_path.touch()

        # Exercise
        failed_downloads = pdp.pre_concatenation_processing(
            mocked_concurrent_download_manifest,
        )
        # Verify
        expected_failed_downloads = ConcurrentDownloadManifest(
            files_reference_data=[
                DownloadDetails(
                    file_name="CROSSREF_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "CROSS/20201218-S945_CROSS_ALL_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/CROSS/CROSSREF_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=35150,
                    md5sum="13da7cea9a7337cd71fd9aea4f909bc6",
                    is_partitioned=False,
                ),
            ],
            whole_files_to_download=[
                DownloadDetails(
                    file_name="CROSSREF_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "CROSS/20201218-S945_CROSS_ALL_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/CROSS/CROSSREF_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=35150,
                    md5sum="13da7cea9a7337cd71fd9aea4f909bc6",
                    is_partitioned=False,
                ),
            ],
            partitions_to_download=[],
        )
        assert failed_downloads == expected_failed_downloads
        # Cleanup - none
        for partition in partitions_to_create:
            partition.unlink()

        crossref_file_path.unlink()

        if not crossref_file_path.exists():
            crossref_destination.replace(crossref_file_path)


class TestConcatenationProcessing:
    def test_concatenation_processing_no_missing_partition_files_scenario(
        self,
        mocked_concurrent_download_manifest,
    ):
        # Setup
        failed_download_manifest = ConcurrentDownloadManifest(
            files_reference_data=[
                DownloadDetails(
                    file_name="CROSSREF_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "CROSS/20201218-S945_CROSS_ALL_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/CROSS/CROSSREF_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=35150,
                    md5sum="13da7cea9a7337cd71fd9aea4f909bc6",
                    is_partitioned=False,
                ),
            ],
            whole_files_to_download=[
                DownloadDetails(
                    file_name="CROSSREF_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "CROSS/20201218-S945_CROSS_ALL_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/CROSS/CROSSREF_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=35150,
                    md5sum="13da7cea9a7337cd71fd9aea4f909bc6",
                    is_partitioned=False,
                ),
            ],
            partitions_to_download=[]
        )
        # Exercise
        concatenated_files = pdp.concatenation_processing(
            mocked_concurrent_download_manifest,
            failed_download_manifest,
        )
        # Verify
        expected_concatenated_files = [
            DownloadDetails(
                file_name="COREREF_945_20201218.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/"
                    "20201218-S945_CORE_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "static_data/post_processing_scenario_1",
                    "2020/12/18/CORE/COREREF_945_20201218.txt.bz2",
                ),
                source_id=945,
                reference_date=datetime.datetime(year=2020, month=12, day=18),
                size=24326963,
                md5sum="8fc8fa1402e23f2d552899525b808514",
                is_partitioned=True,
            ),
            DownloadDetails(
                file_name="WATCHLIST_945_20201218.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/WATCHLIST"
                    "/20201218-S945_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "static_data/post_processing_scenario_1",
                    "2020/12/18/WATCHLIST/WATCHLIST_945_20201218.txt.bz2",
                ),
                source_id=945,
                reference_date=datetime.datetime(year=2020, month=12, day=18),
                size=51648457,
                md5sum="11c5253a7cd1743aea93ec5124fd974d",
                is_partitioned=True,
            ),
        ]
        assert (
            concatenated_files.sort(key=lambda x: x.file_name) ==
            expected_concatenated_files.sort(key=lambda x: x.file_name)
        )
        # Cleanup - none

    def test_concatenation_processing_one_missing_partition_files_scenario(
        self,
        mocked_concurrent_download_manifest,
    ):
        # Setup
        failed_download_manifest = ConcurrentDownloadManifest(
            files_reference_data=[
                DownloadDetails(
                    file_name="WATCHLIST_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "WATCHLIST/20201218-S945_WATCHLIST_username_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/WATCHLIST/WATCHLIST_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=51648457,
                    md5sum="11c5253a7cd1743aea93ec5124fd974d",
                    is_partitioned=True,
                ),
            ],
            whole_files_to_download=[],
            partitions_to_download=[
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=41943041&end=47185920'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_9.txt"
                    ),
                    partition_index=9,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=47185921&end=51648457'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_10.txt"
                    ),
                    partition_index=10,
                ),
            ]
        )
        # Exercise
        concatenated_files = pdp.concatenation_processing(
            mocked_concurrent_download_manifest,
            failed_download_manifest,
        )
        # Verify
        expected_concatenated_files = [
            DownloadDetails(
                file_name="COREREF_945_20201218.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/"
                    "20201218-S945_CORE_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "static_data/post_processing_scenario_1",
                    "2020/12/18/CORE/COREREF_945_20201218.txt.bz2",
                ),
                source_id=945,
                reference_date=datetime.datetime(year=2020, month=12, day=18),
                size=24326963,
                md5sum="8fc8fa1402e23f2d552899525b808514",
                is_partitioned=True,
            ),
        ]
        assert concatenated_files == expected_concatenated_files
        # Cleanup - none

    def test_concatenation_processing_with_no_file_to_concatenate(
        self,
        mocked_concurrent_download_manifest,
    ):
        # Setup
        failed_download_manifest = ConcurrentDownloadManifest(
            files_reference_data=[
                DownloadDetails(
                    file_name="COREREF_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/"
                        "20201218-S945_CORE_ALL_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/CORE/COREREF_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=24326963,
                    md5sum="8fc8fa1402e23f2d552899525b808514",
                    is_partitioned=True,
                ),
                DownloadDetails(
                    file_name="WATCHLIST_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "WATCHLIST/20201218-S945_WATCHLIST_username_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/WATCHLIST/WATCHLIST_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=51648457,
                    md5sum="11c5253a7cd1743aea93ec5124fd974d",
                    is_partitioned=True,
                ),
            ],
            whole_files_to_download=[],
            partitions_to_download=[
                PartitionDownloadDetails(
                    parent_file_name='COREREF_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/'
                        '20201218-S945_CORE_ALL_0_0?start=15728641&end=20971520'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/CORE/"
                        "COREREF_945_20201218_4.txt"
                    ),
                    partition_index=4,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=41943041&end=47185920'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_9.txt"
                    ),
                    partition_index=9,
                ),
            ]
        )
        # Exercise
        concatenated_files = pdp.concatenation_processing(
            mocked_concurrent_download_manifest,
            failed_download_manifest,
        )
        # Verify
        expected_concatenated_files = []
        assert concatenated_files == expected_concatenated_files
        # Cleanup - none


class TestUpdateFailedDownloadManifest:
    def test_update_of_download_manifest_with_no_failed_downloads(
        self,
        mocked_concurrent_download_manifest,
    ):
        # Setup
        initial_download_manifest = mocked_concurrent_download_manifest
        failed_files = []
        failed_download_manifest = ConcurrentDownloadManifest(
            files_reference_data=[],
            whole_files_to_download=[],
            partitions_to_download=[],
        )
        # Exercise
        updated_download_manifest = pdp.update_failed_download_manifest(
            failed_download_manifest,
            initial_download_manifest,
            failed_files
        )
        # Verify
        expected_download_manifest = ConcurrentDownloadManifest(
            files_reference_data=[],
            whole_files_to_download=[],
            partitions_to_download=[],
        )
        assert updated_download_manifest == expected_download_manifest
        # Cleanup - none

    def test_update_of_download_manifest_scenario_2(
        self,
        mocked_concurrent_download_manifest,
    ):
        """Tests the update_failed_download_manifest function.

        Testing scenario:
            - In the pre-concatenation phase no whole files failed the integrity test and
            no file was found to be missing any partition (hence the failed_download_manifest
            ConcurrentDownloadManifest named-tuple consists of two empty lists).
            - In the post-concatenation phase, WATCHLIST_945_20201218 file fails the
            integrity test.

        Expected output:
            A ConcurrentDownloadManifest with the whole_files_reference field consisting
            of the DownloadDetails named-tuple for the WATCHLIST_945_20201218 file, and
            the concurrent_download_manifest filed consisting of PartitionDownloadDetails
            named-tuples, one for each of the partitions the WATCHLIST_945_20201218 file
            was originally split into.
        """
        # Setup
        failed_downloads_manifest = ConcurrentDownloadManifest(
            files_reference_data=[],
            whole_files_to_download=[],
            partitions_to_download=[],
        )
        initial_download_manifest = mocked_concurrent_download_manifest
        integrity_test_failing_files = [
            DownloadDetails(
                file_name="WATCHLIST_945_20201218.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                    "WATCHLIST/20201218-S945_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "static_data/post_processing_scenario_1",
                    "2020/12/18/WATCHLIST/WATCHLIST_945_20201218.txt.bz2",
                ),
                source_id=945,
                reference_date=datetime.datetime(year=2020, month=12, day=18),
                size=51648457,
                md5sum="11c5253a7cd1743aea93ec5124fd974d",
                is_partitioned=True,
            ),
        ]
        # Exercise
        updated_download_manifest = pdp.update_failed_download_manifest(
            failed_downloads_manifest,
            initial_download_manifest,
            integrity_test_failing_files,
        )
        # Verify
        expected_download_manifest = ConcurrentDownloadManifest(
            files_reference_data=[
                DownloadDetails(
                    file_name="WATCHLIST_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "WATCHLIST/20201218-S945_WATCHLIST_username_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/WATCHLIST/WATCHLIST_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=51648457,
                    md5sum="11c5253a7cd1743aea93ec5124fd974d",
                    is_partitioned=True,
                ),
            ],
            whole_files_to_download=[],
            partitions_to_download=[
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=0&end=5242880'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_1.txt"
                    ),
                    partition_index=1,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=5242881&end=10485760'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_2.txt"),
                    partition_index=2,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=10485761&end=15728640'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_3.txt"
                    ),
                    partition_index=3,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=15728641&end=20971520'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_4.txt"
                    ),
                    partition_index=4,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=20971521&end=26214400'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_5.txt"
                    ),
                    partition_index=5,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=26214401&end=31457280'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_6.txt"
                    ),
                    partition_index=6,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=31457281&end=36700160'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_7.txt"
                    ),
                    partition_index=7,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=36700161&end=41943040'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_8.txt"
                    ),
                    partition_index=8,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=41943041&end=47185920'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_9.txt"
                    ),
                    partition_index=9,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=47185921&end=51648457'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_10.txt"
                    ),
                    partition_index=10,
                ),
            ],
        )
        assert updated_download_manifest == expected_download_manifest
        # Cleanup - none

    def test_update_of_download_manifest_scenario_3(
        self,
        mocked_concurrent_download_manifest,
    ):
        """Tests the update_failed_download_manifest function.

        Testing scenario:
            - In the pre-concatenation phase, CROSSREF_945_20201218 failed the data
            integrity test and the WATCHLIST_945_20201218 file had a missing partition (
            hence the failed_download_manifest ConcurrentDownloadManifest named-tuple
            consists of the whole_file_reference field that is a list of the DownloadDetails
            tuples of the two files mentioned above, while the concurrent_download_manifest
            filed is a list containing the missing partition of the WATCHLIST file plus the
            DownloadDetails of the CROSSREF file).
            - In the post-concatenation phase, COREREF_945_20201218 file fails the
            integrity test.

        Expected output:
            A ConcurrentDownloadManifest with the whole_files_reference field consisting
            of the DownloadDetails named-tuple for the CROSSREF, COREREF and WATCHLIST
            files, and the concurrent_download_manifest filed consisting of all the
            PartitionDownloadDetails named-tuples of the COREREF file, a
            PartitionDownloadDetails of the missing partition of the WATCHLIST file, and
            the DownloadDetails named-tuple of the CROSSREF file.
        """
        # Setup
        initial_download_manifest = mocked_concurrent_download_manifest
        integrity_test_failing_files = [
            DownloadDetails(
                file_name="COREREF_945_20201218.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/"
                    "20201218-S945_CORE_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "static_data/post_processing_scenario_1",
                    "2020/12/18/CORE/COREREF_945_20201218.txt.bz2",
                ),
                source_id=945,
                reference_date=datetime.datetime(year=2020, month=12, day=18),
                size=24326963,
                md5sum="8fc8fa1402e23f2d552899525b808514",
                is_partitioned=True,
            ),
        ]
        failed_downloads_manifest = ConcurrentDownloadManifest(
            files_reference_data=[
                DownloadDetails(
                    file_name="CROSSREF_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "CROSS/20201218-S945_CROSS_ALL_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/CROSS/CROSSREF_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=35150,
                    md5sum="13da7cea9a7337cd71fd9aea4f909bc6",
                    is_partitioned=False,
                ),
                DownloadDetails(
                    file_name="WATCHLIST_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "WATCHLIST/20201218-S945_WATCHLIST_username_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/WATCHLIST/WATCHLIST_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=51648457,
                    md5sum="11c5253a7cd1743aea93ec5124fd974d",
                    is_partitioned=True,
                ),
            ],
            whole_files_to_download=[
                DownloadDetails(
                    file_name="CROSSREF_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "CROSS/20201218-S945_CROSS_ALL_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/CROSS/CROSSREF_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=35150,
                    md5sum="13da7cea9a7337cd71fd9aea4f909bc6",
                    is_partitioned=False,
                ),
            ],
            partitions_to_download=[
                DownloadDetails(
                    file_name="CROSSREF_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "CROSS/20201218-S945_CROSS_ALL_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/CROSS/CROSSREF_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=35150,
                    md5sum="13da7cea9a7337cd71fd9aea4f909bc6",
                    is_partitioned=False,
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=47185921&end=51648457'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_10.txt"
                    ),
                    partition_index=10,
                ),
            ],
        )
        # Exercise
        updated_download_manifest = pdp.update_failed_download_manifest(
            failed_downloads_manifest,
            initial_download_manifest,
            integrity_test_failing_files,
        )
        # Verify
        expected_download_manifest = ConcurrentDownloadManifest(
            files_reference_data=[
                DownloadDetails(
                    file_name="CROSSREF_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "CROSS/20201218-S945_CROSS_ALL_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/CROSS/CROSSREF_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=35150,
                    md5sum="13da7cea9a7337cd71fd9aea4f909bc6",
                    is_partitioned=False,
                ),
                DownloadDetails(
                    file_name="WATCHLIST_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "WATCHLIST/20201218-S945_WATCHLIST_username_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/WATCHLIST/WATCHLIST_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=51648457,
                    md5sum="11c5253a7cd1743aea93ec5124fd974d",
                    is_partitioned=True,
                ),
                DownloadDetails(
                    file_name="COREREF_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/"
                        "20201218-S945_CORE_ALL_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/CORE/COREREF_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=24326963,
                    md5sum="8fc8fa1402e23f2d552899525b808514",
                    is_partitioned=True,
                ),
            ],
            whole_files_to_download=[
                DownloadDetails(
                    file_name="CROSSREF_945_20201218.txt.bz2",
                    download_url=(
                        "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/"
                        "CROSS/20201218-S945_CROSS_ALL_0_0"
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1",
                        "2020/12/18/CROSS/CROSSREF_945_20201218.txt.bz2",
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(year=2020, month=12, day=18),
                    size=35150,
                    md5sum="13da7cea9a7337cd71fd9aea4f909bc6",
                    is_partitioned=False,
                ),
            ],
            partitions_to_download=[
                DownloadDetails(
                    file_name='CROSSREF_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'CROSS/20201218-S945_CROSS_ALL_0_0'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/CROSS/"
                        "CROSSREF_945_20201218.txt.bz2"
                    ),
                    source_id=945,
                    reference_date=datetime.datetime(2020, 12, 18, 0, 0),
                    size=35150,
                    md5sum='13da7cea9a7337cd71fd9aea4f909bc6',
                    is_partitioned=False
                ),
                PartitionDownloadDetails(
                    parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                        'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=47185921&end=51648457'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                        "WATCHLIST_945_20201218_10.txt"
                    ),
                    partition_index=10,
                ),
                PartitionDownloadDetails(
                    parent_file_name='COREREF_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/'
                        '20201218-S945_CORE_ALL_0_0?start=0&end=5242880'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/CORE/"
                        "COREREF_945_20201218_1.txt"
                    ),
                    partition_index=1,
                ),
                PartitionDownloadDetails(
                    parent_file_name='COREREF_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/'
                        '20201218-S945_CORE_ALL_0_0?start=5242881&end=10485760'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/CORE/"
                        "COREREF_945_20201218_2.txt"
                    ),
                    partition_index=2,
                ),
                PartitionDownloadDetails(
                    parent_file_name='COREREF_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/'
                        '20201218-S945_CORE_ALL_0_0?start=10485761&end=15728640'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/CORE/"
                        "COREREF_945_20201218_3.txt"
                    ),
                    partition_index=3,
                ),
                PartitionDownloadDetails(
                    parent_file_name='COREREF_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/'
                        '20201218-S945_CORE_ALL_0_0?start=15728641&end=20971520'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/CORE/"
                        "COREREF_945_20201218_4.txt"
                    ),
                    partition_index=4,
                ),
                PartitionDownloadDetails(
                    parent_file_name='COREREF_945_20201218.txt.bz2',
                    download_url=(
                        'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/'
                        '20201218-S945_CORE_ALL_0_0?start=20971521&end=24326963'
                    ),
                    file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                        "static_data/post_processing_scenario_1/2020/12/18/CORE/"
                        "COREREF_945_20201218_5.txt"
                    ),
                    partition_index=5,
                ),
            ],
        )
        assert updated_download_manifest == expected_download_manifest
        # Cleanup - none
