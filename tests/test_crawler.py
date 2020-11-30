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
