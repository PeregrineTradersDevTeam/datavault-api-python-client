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
