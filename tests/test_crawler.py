import pytest

from datavault_api_client import crawler
from datavault_api_client.data_structures import DiscoveredFileInfo


class TestCleanRawFilename:
    @pytest.mark.parametrize(
        "datavault_assigned_name, expected_cleaned_filename", [
            ("WATCHLIST_username_676_20200610.txt.bz2", "WATCHLIST_676_20200610.txt.bz2"),
            ("CROSSREF_903_20200610.txt.bz2", "CROSSREF_903_20200610.txt.bz2"),
            ("COREREF_945_20200610.txt.bz2", "COREREF_945_20200610.txt.bz2"),
            ("REPLAY_794_20200316.txt.bz2", "REPLAY_794_20200316.txt.bz2"),
            ("SEDOL_794_20200316.txt.bz2", "SEDOL_794_20200316.txt.bz2"),
            ("CUSIP_794_20200316.txt.bz2", "CUSIP_794_20200316.txt.bz2"),
            ("PREMREF_794_20200316.txt.bz2", "PREMREF_794_20200316.txt.bz2"),
        ],
    )
    def test_name_cleaning(self, datavault_assigned_name, expected_cleaned_filename):
        # Setup - none
        # Exercise
        cleaned_filename = crawler.clean_raw_filename(datavault_assigned_name)
        # Verify
        assert cleaned_filename == expected_cleaned_filename
        # Cleanup - none


class TestParseSourceFromName:
    def test_source_id_parser(self):
        # Setup
        filename_to_parse = "WATCHLIST_945_20201201.txt.bz2"
        # Exercise
        parsed_source_id = crawler.parse_source_from_name(filename_to_parse)
        # Verify
        expected_source_id = "945"
        assert parsed_source_id == expected_source_id
        # Cleanup - none


class TestCreateDiscoveredFileObject:
    def test_discovered_file_object_creation(self):
        # Setup
        file_node = {
            'name': 'WATCHLIST_accountname_945_20201130.txt.bz2',
            'fid': '20201130-S945_WATCHLIST_accountname_0_0',
            'parent': '/v2/list/2020/11/30/S945/WATCHLIST',
            'url': '/v2/data/2020/11/30/S945/WATCHLIST/20201130-S945_WATCHLIST_accountname_0_0',
            'size': 78994869,
            'md5sum': 'bf703f867cad0b414d84fac0c9bfe0e5',
            'createdAt': '2020-11-30T23:22:36',
            'updatedAt': '2020-11-30T23:22:36',
            'writable': False,
            'directory': False
        }
        # Exercise
        created_discovered_file_object = crawler.create_discovered_file_object(file_node)
        # Verify
        expected_discovered_file_object = DiscoveredFileInfo(
            file_name='WATCHLIST_945_20201130.txt.bz2',
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/11/30/S945/WATCHLIST/"
                "20201130-S945_WATCHLIST_accountname_0_0"
            ),
            source="945",
            size=78994869,
            md5sum="bf703f867cad0b414d84fac0c9bfe0e5",
        )
        assert created_discovered_file_object == expected_discovered_file_object
        # Cleanup - none


class TestDatavaultCrawl:
    def test_crawler_with_instrument_level_url(
        self,
        mocked_datavault_api_instrument_level,
        mocked_set_of_files_available_to_download_single_instrument,
    ):
        # Setup
        url_to_crawl = (
            "https://api.icedatavault.icedataservices.com/v2/list/2020/07/16/S367/WATCHLIST"
        )
        credentials = ("username", "password")
        # Exercise
        discovered_files = crawler.datavault_crawler(url_to_crawl, credentials)
        # Verify
        assert discovered_files == mocked_set_of_files_available_to_download_single_instrument
        # Cleanup - none

    def test_crawler_with_single_source_and_single_day_setup(
        self,
        mocked_datavault_api_single_source_single_day,
        mocked_set_of_files_available_to_download_single_source_single_day,
    ):
        # Setup
        url_to_crawl = "https://api.icedatavault.icedataservices.com/v2/list"
        credentials = ("username", "password")
        # Exercise
        discovered_files = crawler.datavault_crawler(url_to_crawl, credentials)
        # Verify
        expected_files = mocked_set_of_files_available_to_download_single_source_single_day
        expected_files.sort(key=lambda x: x.file_name, reverse=True)
        assert discovered_files == expected_files
        # Cleanup - none

    def test_crawler_with_single_source_and_multiple_days_setup(
        self,
        mocked_datavault_api_single_source_multiple_days,
        mocked_set_of_files_available_to_download_single_source_multiple_days,
    ):
        # Setup
        url_to_crawl = "https://api.icedatavault.icedataservices.com/v2/list"
        credentials = ("username", "password")
        # Exercise
        discovered_files = crawler.datavault_crawler(url_to_crawl, credentials)
        discovered_files.sort(key=lambda x: x.file_name)
        # Verify
        expected_files = mocked_set_of_files_available_to_download_single_source_multiple_days
        expected_files.sort(key=lambda x: x.file_name)
        assert discovered_files == expected_files
        # Cleanup - none

    def test_crawler_under_multiple_sources_and_single_day_scenario(
        self,
        mocked_datavault_api_multiple_sources_single_day,
        mocked_set_of_files_available_to_download_multiple_sources_single_day
    ):
        # Setup
        url_to_crawl = "https://api.icedatavault.icedataservices.com/v2/list"
        credentials = ("username", "password")
        # Exercise
        discovered_files = crawler.datavault_crawler(url_to_crawl, credentials)
        discovered_files.sort(key=lambda x: x.file_name)
        # Verify
        expected_files = mocked_set_of_files_available_to_download_multiple_sources_single_day
        expected_files.sort(key=lambda x: x.file_name)
        assert discovered_files == expected_files
        # Cleanup - none

    def test_crawler_under_select_source_scenario(
        self,
        mocked_datavault_api_multiple_sources_single_day,
        mocked_set_of_files_available_to_download_multiple_sources_single_day,
    ):
        # Setup
        url_to_crawl = "https://api.icedatavault.icedataservices.com/v2/list"
        credentials = ("username", "password")
        # Exercise
        discovered_files = crawler.datavault_crawler(url_to_crawl, credentials, source_id="207")
        discovered_files.sort(key=lambda x: x.file_name)
        # Verify
        expected_files = [
            file for file in mocked_set_of_files_available_to_download_multiple_sources_single_day
            if file.source == "207"
        ]
        expected_files.sort(key=lambda x: x.file_name)
        assert discovered_files == expected_files
        # Cleanup - none
