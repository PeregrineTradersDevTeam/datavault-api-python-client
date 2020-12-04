import hashlib
import os
import pathlib

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
