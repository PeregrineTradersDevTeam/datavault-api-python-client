import hashlib
import os
import pathlib
import datetime
import pytest

from datavault_api_client import data_integrity
from datavault_api_client.data_structures import DownloadDetails


class TestChecksum:
    @pytest.mark.parametrize(
        'file_path, hash_constructor, true_digest', [
            (pathlib.Path(__file__).resolve().parent / 'static_data' / 'test_file_1.txt',
             hashlib.md5, '6bde2aa6394fde37e21748bc0578113b'),
            (pathlib.Path(__file__).resolve().parent / 'static_data' / 'test_file_1.txt',
             hashlib.sha1, '461d4595a1bda35c1a1534cb9b2bfc3b62e84b47'),
            (pathlib.Path(__file__).resolve().parent / 'static_data' / 'test_file_1.txt',
             hashlib.sha256, '13aea96040f2133033d103008d5d96cfe98b3361f7202d77bea97b2424a7a6cd'),
            (pathlib.Path(__file__).resolve().parent / 'static_data' / 'test_file_1.txt',
             hashlib.sha512, (
                 'bdd1863ef1cddbd43af1abc086ec052fb26ce787cbfa6c99c545cdc238b722dbe958e51'
                 '9db2baafca5c25692ee30bb83f18d4d1fa790d79d4da11e3b5f14ac1a'
             )
             )
        ]
    )
    def test_correct_digest_calculation(self, file_path, hash_constructor, true_digest):
        # Setup - none
        # Exercise
        calculated_digest = data_integrity.calculate_checksum(file_path, hash_constructor)
        # Verify
        assert calculated_digest == true_digest
        # Cleanup - none


class TestCheckSize:
    @pytest.mark.parametrize(
        'file_path, true_file_size', [
            (
                pathlib.Path(__file__).resolve().parent / 'static_data' / 'test_file_1.txt',
                2000000,
            ),
            (
                pathlib.Path(__file__).resolve().parent / 'static_data' / 'test_file_2.txt.bz2',
                10567426,
             )
        ]
    )
    def test_right_size_calculation(self, file_path, true_file_size):
        # Setup - none
        # Exercise
        calculated_size = data_integrity.check_size(file_path)
        # Verify
        assert calculated_size == true_file_size
        # Cleanup - none


class TestDataIntegrityTest:
    def test_integrity_testing_fail_scenario(self):
        # Setup
        file_download_details = DownloadDetails(
            file_name='WATCHLIST_367_20200721.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/'
                'WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
            ),
            file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                'mock_test_data', '2020', '07', '21', 'S367', 'WATCHLIST',
                'WATCHLIST_367_20200721.txt.bz2',
            ),
            source_id=367,
            reference_date=datetime.datetime(year=2020, month=7, day=21),
            size=82451354,
            md5sum='62df718ef5eb5f9f1ea3f6ea1f826c30',
            is_partitioned=True
        )
        file_directory = file_download_details.file_path.parent
        file_directory.mkdir(parents=True, exist_ok=True)
        with file_download_details.file_path.open('wb') as outfile:
            random_file_content = os.urandom(5000)
            outfile.write(random_file_content)
        # Exercise
        outcome = data_integrity.data_integrity_test(file_download_details)
        # Verify
        expected_outcome = False
        assert outcome == expected_outcome
        # Cleanup - none
        # First, remove all the created files
        for file in list(file_directory.glob('**/*.bz2')):
            file.unlink()
        # Then remove all the created folders iteratively:
        directory_root = pathlib.Path(__file__).resolve().parent / 'mock_test_data'
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()

    def test_integrity_testing_pass_scenario(self):
        # Setup
        file_download_details = DownloadDetails(
            file_name='CROSSREF_367_20200721.txt.bz2',
            download_url='https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/'
                         'S367/CROSS/20200721-S367_CROSS_ALL_0_0',
            file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                'static_data', 'CROSSREF_367_20200721_simulated.txt.bz2'
            ),
            source_id=367,
            reference_date=datetime.datetime(year=2020, month=7, day=21),
            size=924,
            md5sum='d742203115d9637199386ac8d71cc4cd',
            is_partitioned=False)
        # Exercise
        outcome = data_integrity.data_integrity_test(file_download_details)
        # Verify
        expected_outcome = True
        assert outcome == expected_outcome
        # Cleanup - none


class TestGetListOfFailedDownloads:
    def test_generation_of_failed_downloads_list(self):
        # Setup
        list_of_file_download_details = [
            DownloadDetails(
                file_name='WATCHLIST_367_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/'
                    'WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'mock_data', '2020', '07', '21', 'S367', 'WATCHLIST',
                    'WATCHLIST_367_20200721.txt.bz2'
                ),
                source_id=367,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=82451354,
                md5sum='62df718ef5eb5f9f1ea3f6ea1f826c30',
                is_partitioned=True,
            ),
            DownloadDetails(
                file_name='CROSSREF_367_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/'
                    'S367/CROSS/20200721-S367_CROSS_ALL_0_0'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'static_data', 'CROSSREF_367_20200721_simulated.txt.bz2'
                ),
                source_id=367,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=924,
                md5sum='d742203115d9637199386ac8d71cc4cd',
                is_partitioned=False,
            )
        ]

        file_directory = pathlib.Path(__file__).resolve().parent.joinpath(
            'mock_data', '2020', '07', '21', 'S367', 'WATCHLIST',
        )
        file_directory.mkdir(parents=True, exist_ok=True)
        path_to_watchlist_file = file_directory / 'WATCHLIST_367_20200721.txt.bz2'
        with path_to_watchlist_file.open('wb') as outfile:
            random_file_content = os.urandom(5000)
            outfile.write(random_file_content)
        # Exercise
        failed_downloads = data_integrity.get_list_of_failed_downloads(
            list_of_file_download_details,
        )
        # Verify
        expected_failed_downloads = [
            DownloadDetails(
                file_name='WATCHLIST_367_20200721.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020'
                    '/07/21/S367/WATCHLIST/20200721-S367_WATCHLIST_username_0_0'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'mock_data', '2020', '07', '21', 'S367', 'WATCHLIST',
                    'WATCHLIST_367_20200721.txt.bz2',
                ),
                source_id=367,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=82451354,
                md5sum='62df718ef5eb5f9f1ea3f6ea1f826c30',
                is_partitioned=True,
            ),
            ]
        assert failed_downloads == expected_failed_downloads
        # Cleanup
        for file in list(file_directory.glob('**/*.bz2')):
            file.unlink()
        directory_root = pathlib.Path(__file__).resolve().parent / 'mock_data'
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()
