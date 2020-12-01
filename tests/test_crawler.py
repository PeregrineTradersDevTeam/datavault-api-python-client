import pytest

from datavault_api_client import crawler


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
