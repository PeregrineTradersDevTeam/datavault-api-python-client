import os
import pathlib

import pytest

from datavault_api_client import post_download_processing as pdp
from datavault_api_client.data_structures import DownloadDetails, PartitionDownloadDetails


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
        file_name = 'WATCHLIST_367_20200721.txt.bz2'
        files_and_partitions = (
            mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day
        )
        # Exercise
        computed_list_of_name_specific_partitions = pdp.get_partitions_download_details(
                files_and_partitions, file_name,
        )
        # Verify
        expected_names_of_specific_partitions = [
            item for item in files_and_partitions
            if type(item) is PartitionDownloadDetails and item.parent_file_name == file_name
        ]
        assert computed_list_of_name_specific_partitions == expected_names_of_specific_partitions
        # Cleanup - none


class TestGetPartitionIndex:
    @pytest.mark.parametrize(
        'path_to_partition_file, correct_partition_index', [
            (
                pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '22', 'S905', 'WATCHLIST', 'WATCHLIST_905_20200722_5.txt',
                ),
                5,
            ),
            (
                pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '22', 'S905', 'WATCHLIST',
                    'WATCHLIST_905_20200722_20.txt',
                ),
                20,
            ),
            (
                pathlib.Path(__file__).resolve().parent.joinpath(
                    'Data', '2020', '07', '22', 'S905', 'WATCHLIST',
                    'WATCHLIST_905_20200722_27.txt',
                ),
                27,
            ),
        ]
    )
    def test_identification_of_partition_index(
        self,
        path_to_partition_file,
        correct_partition_index
    ):
        # Setup - none
        # Exercise
        partition_index = pdp.get_partition_index(path_to_partition_file)
        # Verify
        assert partition_index == correct_partition_index
        # Cleanup - none


class TestGetListOfDownloadedPartitions:
    def test_retrieval_of_downloaded_partitions(self, simulated_downloaded_partitions):
        # Setup
        # Exercise
        computed_list_of_downloaded_partitions = pdp.get_list_of_downloaded_partitions(
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
            "static_data", "temp_folder"
        )
        directory.mkdir(exist_ok=True)
        # Exercise
        downloaded_partitions = pdp.get_list_of_downloaded_partitions(
            directory,
        )
        # Verify
        assert downloaded_partitions == []
        # Cleanup
        directory.rmdir()


class TestGetListOfMissingPartitions:
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
            size=82451354,
            md5sum='62df718ef5eb5f9f1ea3f6ea1f826c30',
            is_partitioned=True,
        )
        # Exercise
        computed_missing_partitions = pdp.get_list_of_missing_partitions(
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


class TestGetAllMissingPartitionsAndCorrespondingFileReferences:
    def test_identification_of_missing_partitions_and_files_with_incomplete_download(
        self,
        mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
        mocked_download_details_multiple_sources_single_day
    ):
        # Setup
        base_path = pathlib.Path(__file__).resolve().parent.joinpath(
            "Data", "2020", "07", "21",
        )
        downloaded_instruments__directories = [
            base_path / 'S207' / 'CROSS',
            base_path / 'S207' / 'WATCHLIST',
            base_path / 'S367' / 'WATCHLIST',
            ]
        for directory in downloaded_instruments__directories:
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
        files_with_missing_partitions, missing_partitions = (
            pdp.get_all_missing_partitions_and_corresponding_file_references(
                mocked_download_details_multiple_sources_single_day,
                mocked_list_of_whole_files_and_partitions_download_details_multiple_sources_single_day,
            )
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
                size=82451354,
                md5sum='62df718ef5eb5f9f1ea3f6ea1f826c30',
                is_partitioned=True)
        ]
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
            )
        ]
        assert files_with_missing_partitions == expected_files_with_missing_partitions
        assert missing_partitions == expected_missing_partitions
        # Cleanup
        # First, remove all the created files
        for file in list(base_path.glob('**/*.txt')):
            file.unlink()
        # Then remove all the created folders iteratively:
        directory_root = pathlib.Path(__file__).resolve().parent / 'Data'
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()

