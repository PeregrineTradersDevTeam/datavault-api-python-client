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
