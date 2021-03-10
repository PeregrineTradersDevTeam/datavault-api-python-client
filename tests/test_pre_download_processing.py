import pathlib
import datetime
import pytest
import json
from datavault_api_client import pre_download_processing as pdp
from datavault_api_client.data_structures import DownloadDetails, ItemToDownload


class TestGenerateFilePathMatchingDatavaultStructure:
    def test_file_path_generation(self):
        # Setup
        file_download_url = (
            "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S905/WATCHLIST/"
            "20200722-S905_WATCHLIST_username_0_0"
        )
        path_to_data_folder = pathlib.Path(__file__).resolve().parent.joinpath("Data").as_posix()
        file_name = "WATCHLIST_905_20200722.txt.bz2"
        # Exercise
        generated_file_path = pdp.generate_file_path_matching_datavault_structure(
            path_to_data_folder,
            file_name,
            file_download_url,
        )
        # Verify
        expected_file_path = pathlib.Path(__file__).resolve().parent.joinpath(
            "Data", "2020", "07", "22", "S905", "WATCHLIST", "WATCHLIST_905_20200722.txt.bz2"
        )
        assert generated_file_path == expected_file_path
        # Cleanup - none


class TestConvertMbToBytes:
    @pytest.mark.parametrize(
        "size_in_mib, correct_size_in_bytes",
        [(5.0, 5242880), (5.3, 5557453), (10.0, 10485760), (13.2, 13841203)],
    )
    def test_conversion_of_mb_in_bytes(self, size_in_mib, correct_size_in_bytes):
        # Setup - none
        # Exercise
        calculated_size_in_bytes = pdp.convert_mib_to_bytes(size_in_mib)
        # Verify
        assert calculated_size_in_bytes == correct_size_in_bytes
        # Cleanup - none


class TestCalculateMultiPartThreshold:
    @pytest.mark.parametrize(
        "partition_size_in_mib, correct_multi_part_threshold",
        [(5.0, 14680064), (5.3, 15560868), (10.0, 29360128), (13.2, 38755368)],
    )
    def test_calculation_of_multi_part_threshold(
        self, partition_size_in_mib, correct_multi_part_threshold
    ):
        # Setup - none
        # Exercise
        calculated_multi_part_threshold = pdp.calculate_multi_part_threshold(partition_size_in_mib)
        # Verify
        assert calculated_multi_part_threshold == correct_multi_part_threshold
        # Cleanup - none


class TestCheckIfPartitioned:
    @pytest.mark.parametrize(
        "file_size, partition_size_in_mib, expected_flag",
        [
            (23383245, 5.0, True),
            (12897485, 5.0, False),
            (48024781, 13.2, True),
            (34183577, 13.2, False),
        ],
    )
    def test_identification_of_files_to_split(
        self, file_size, partition_size_in_mib, expected_flag,
    ):
        # Setup - none
        # Exercise
        calculated_partitioned_flag = pdp.check_if_partitioned(
            file_size, partition_size_in_mib
        )
        # Verify
        assert calculated_partitioned_flag == expected_flag
        # Cleanup - none


class TestProcessRawDownloadInfo:
    def test_processing_of_raw_download_info_with_synchronous_flag_set_to_false(
        self,
        mocked_files_available_to_download_single_instrument
    ):
        # Setup
        path_to_data_folder = pathlib.Path(__file__).resolve().parent / "Data"
        size_of_partition_in_mib = 5.0
        # Exercise
        for file_details in mocked_files_available_to_download_single_instrument:
            processed_download_details = pdp.process_raw_download_info(
                file_details, path_to_data_folder, size_of_partition_in_mib
            )
        # Verify
        correct_download_details = DownloadDetails(
            file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0"
            ),
            file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                "Data", "2020", "07", "16", "S367", "WATCHLIST", "WATCHLIST_367_20200716.txt.bz2"
            ),
            source_id=367,
            reference_date=datetime.datetime(year=2020, month=7, day=16),
            size=100145874,
            md5sum="fb34325ec9262adc74c945a9e7c9b465",
            is_partitioned=True,
        )
        # noinspection PyUnboundLocalVariable
        assert processed_download_details == correct_download_details
        # Cleanup - none

    def test_processing_of_raw_download_info_without_partition_size(
        self,
        mocked_files_available_to_download_single_instrument
    ):
        # Setup
        path_to_data_folder = pathlib.Path(__file__).resolve().parent / "Data"
        # Exercise
        for file_details in mocked_files_available_to_download_single_instrument:
            processed_download_details = pdp.process_raw_download_info(
                file_details, path_to_data_folder,
            )
        # Verify
        correct_download_details = DownloadDetails(
            file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0"
            ),
            file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                "Data", "2020", "07", "16", "S367", "WATCHLIST", "WATCHLIST_367_20200716.txt.bz2"
            ),
            source_id=367,
            reference_date=datetime.datetime(year=2020, month=7, day=16),
            size=100145874,
            md5sum="fb34325ec9262adc74c945a9e7c9b465",
            is_partitioned=None,
        )
        # noinspection PyUnboundLocalVariable
        assert processed_download_details == correct_download_details
        # Cleanup - none


class TestProcessAllDiscoveredFilesInfo:
    def test_processing_of_all_discovered_files_info_with_synchronous_flag_set_to_false(
        self,
        mocked_files_available_to_download_single_source_single_day,
        mocked_whole_files_download_details_single_source_single_day,
    ):
        # Setup
        path_to_data_folder = pathlib.Path(__file__).resolve().parent / "Data"
        partition_size_in_mb = 5.0
        # Exercise
        list_of_processed_download_details = pdp.process_all_discovered_files_info(
            mocked_files_available_to_download_single_source_single_day,
            path_to_data_folder,
            partition_size_in_mb,
        )
        # Verify
        expected_result = (
            mocked_whole_files_download_details_single_source_single_day
        )
        assert list_of_processed_download_details == expected_result
        # Cleanup - none

    def test_processing_of_all_discovered_files_info_with_synchronous_flag_set_to_true(
        self,
        mocked_files_available_to_download_single_source_single_day,
        mocked_whole_files_download_details_single_source_single_day_synchronous_case,
    ):
        # Setup
        path_to_data_folder = pathlib.Path(__file__).resolve().parent / "Data"
        # Exercise
        list_of_processed_download_details = pdp.process_all_discovered_files_info(
            mocked_files_available_to_download_single_source_single_day,
            path_to_data_folder,
        )
        # Verify
        expected_result = (
            mocked_whole_files_download_details_single_source_single_day_synchronous_case
        )
        assert list_of_processed_download_details == expected_result
        # Cleanup - none

    def test_processing_of_all_discovered_files_info_on_multiple_days_omitting_partition_size(
        self,
        mocked_files_available_to_download_single_source_multiple_days,
        mocked_download_info_single_source_multiple_days_synchronous,
    ):
        # Setup
        path_to_data_folder = pathlib.Path(__file__).resolve().parent / "Temp" / "Data"
        # Exercise
        list_of_processed_download_details = pdp.process_all_discovered_files_info(
            mocked_files_available_to_download_single_source_multiple_days,
            path_to_data_folder,
        )
        # Verify
        expected_result = mocked_download_info_single_source_multiple_days_synchronous
        assert list_of_processed_download_details == expected_result
        # Cleanup - none

    def test_processing_of_all_discovered_files_info_on_multiple_days_with_synchronous_flag_off(
        self,
        mocked_files_available_to_download_single_source_multiple_days,
        mocked_download_info_single_source_multiple_days_concurrent,
    ):
        # Setup
        path_to_data_folder = pathlib.Path(__file__).resolve().parent / "Temp" / "Data"
        # Exercise
        list_of_processed_download_details = pdp.process_all_discovered_files_info(
            mocked_files_available_to_download_single_source_multiple_days,
            path_to_data_folder,
            partition_size_in_mib=5.0,
        )
        # Verify
        expected_result = mocked_download_info_single_source_multiple_days_concurrent
        assert list_of_processed_download_details == expected_result
        # Cleanup - none


class TestDownloadDetailToDict:
    def test_conversion_to_typed_dict(self):
        # Setup
        download_details_to_convert = DownloadDetails(
            file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0"
            ),
            file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                "Data", "2020", "07", "16", "S367", "WATCHLIST", "WATCHLIST_367_20200716.txt.bz2"
            ),
            source_id=367,
            reference_date=datetime.datetime(year=2020, month=7, day=16),
            size=100145874,
            md5sum="fb34325ec9262adc74c945a9e7c9b465",
            is_partitioned=True,
        )
        # Exercise
        converted_info = pdp.download_detail_to_dict(download_details_to_convert)
        # Verify
        expected_info = ItemToDownload(
            file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0"
            ),
            file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                "Data", "2020", "07", "16", "S367", "WATCHLIST", "WATCHLIST_367_20200716.txt.bz2"
            ).as_posix(),
            source_id=367,
            reference_date="2020-07-16T00:00:00",
            size=100145874,
            md5sum="fb34325ec9262adc74c945a9e7c9b465",
        )
        assert converted_info == expected_info
        # Cleanup - none


class TestFilterDateSpecificInfo:
    def test_filtering_of_date_specific_info(
        self,
        mocked_download_info_single_source_multiple_days_concurrent,
    ):
        # Setup
        # Exercise
        date_specific_info = pdp.filter_date_specific_info(
            mocked_download_info_single_source_multiple_days_concurrent,
            datetime.datetime(year=2020, month=7, day=20),
        )
        # Verify
        expected_info = [
            ItemToDownload(
                file_name='COREREF_207_20200720.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/CORE/'
                    '20200720-S207_CORE_ALL_0_0'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Temp/Data/', '2020/07/20/S207/CORE/COREREF_207_20200720.txt.bz2'
                ).as_posix(),
                source_id=207,
                reference_date="2020-07-20T00:00:00",
                size=4548016,
                md5sum='a46a5f07b6a402d4023ef550df6a12e4',
            ),
            ItemToDownload(
                file_name='CROSSREF_207_20200720.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/CROSS/'
                    '20200720-S207_CROSS_ALL_0_0'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Temp/Data/', '2020/07/20/S207/CROSS/CROSSREF_207_20200720.txt.bz2'
                ).as_posix(),
                source_id=207,
                reference_date="2020-07-20T00:00:00",
                size=14571417,
                md5sum='6b3dbd152e7dccf4147f62b6ce1c78c3',
            ),
            ItemToDownload(
                file_name='WATCHLIST_207_20200720.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/'
                    'WATCHLIST/20200720-S207_WATCHLIST_username_0_0'
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    'Temp/Data/', '2020/07/20/S207/WATCHLIST/WATCHLIST_207_20200720.txt.bz2'
                ).as_posix(),
                source_id=207,
                reference_date="2020-07-20T00:00:00",
                size=70613654,
                md5sum='ba2c00511520a3cf4b5383ceedb3b41d',
            ),
        ]
        assert date_specific_info == expected_info
        # Cleanup - none

    def test_filtering_of_date_specific_info_with_no_match(self):
        # Setup
        download_details = [
            DownloadDetails(
                file_name="WATCHLIST_367_20201214.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/12/14/S367/"
                    "WATCHLIST/20201214-S367_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data", "2020", "12", "14", "S367", "WATCHLIST",
                    "WATCHLIST_367_20201214.txt.bz2"
                ),
                source_id=367,
                reference_date=datetime.datetime(year=2020, month=12, day=14),
                size=100145874,
                md5sum="fb34325ec9262adc74c945a9e7c9b465",
                is_partitioned=True,
            ),
        ]
        # Exercise
        date_specific_info = pdp.filter_date_specific_info(
            download_details,
            datetime.datetime(year=2020, month=12, day=15),
        )
        # Verify
        assert date_specific_info == []
        # Cleanup - none


class TestGenerateDateSpecificPath:
    def test_generation_of_date_specific_path(self):
        # Setup
        item_to_download = ItemToDownload(
            file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0"
            ),
            file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                "Data", "2020", "07", "16", "S367", "WATCHLIST", "WATCHLIST_367_20200716.txt.bz2"
            ).as_posix(),
            source_id=367,
            reference_date="2020-07-16T00:00:00",
            size=100145874,
            md5sum="fb34325ec9262adc74c945a9e7c9b465",
        )
        # Exercise
        date_specific_path = pdp.generate_date_specific_path(item_to_download)
        # Verify
        expected_path = pathlib.Path(__file__).resolve().parent.joinpath(
            "Data", "2020", "07", "16", "download_manifest_20200716.json",
        ).as_posix()
        assert date_specific_path == expected_path
        # Cleanup - none


class TestWriteManifestToJson:
    def test_json_writer(self):
        # Setup
        file_payload = [
            ItemToDownload(
                file_name="WATCHLIST_367_20201211.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/12/11/S367/"
                    "WATCHLIST/20201211-S367_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data", "2020", "12", "11", "S367", "WATCHLIST",
                    "WATCHLIST_367_20201211.txt.bz2"
                ).as_posix(),
                source_id=367,
                reference_date="2020-12-11T00:00:00",
                size=100145874,
                md5sum="fb34325ec9262adc74c945a9e7c9b465",
            ),
        ]
        path_to_outfile = pathlib.Path(__file__).resolve().parent.joinpath(
            "static_data", "Temp", "2020", "12", "11", "download_manifest_20201211.json"
        )
        # Exercise
        pdp.write_manifest_to_json(file_payload, path_to_outfile)
        # Verify
        with path_to_outfile.open("r") as infile:
            file_content = json.load(infile)
        assert file_content == file_payload
        # Cleanup - none
        path_to_outfile.unlink()
        directory_root = pathlib.Path(__file__).resolve().parent / "static_data" / "Temp"
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()

    def test_json_writer_with_existing_directory(self):
        # Setup
        file_payload = [
            ItemToDownload(
                file_name="WATCHLIST_367_20201211.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/12/11/S367/"
                    "WATCHLIST/20201211-S367_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data", "2020", "12", "11", "S367", "WATCHLIST",
                    "WATCHLIST_367_20201211.txt.bz2"
                ).as_posix(),
                source_id=367,
                reference_date="2020-12-11T00:00:00",
                size=100145874,
                md5sum="fb34325ec9262adc74c945a9e7c9b465",
            ),
        ]
        path_to_outfile = pathlib.Path(__file__).resolve().parent.joinpath(
            "static_data", "download_manifest_20201211.json"
        )
        # Exercise
        pdp.write_manifest_to_json(file_payload, path_to_outfile)
        # Verify
        with path_to_outfile.open("r") as infile:
            file_content = json.load(infile)
        assert file_content == file_payload
        # Cleanup - none
        path_to_outfile.unlink()

    def test_manifest_writer_with_existing_manifest_file(self):
        """Tests the behaviour of the manifest writer when a manifest file already exists.
        """
        # Setup
        existing_manifest_content = [
            {
                'file_name': 'WATCHLIST_207_20210212.txt.bz2',
                'download_url': (
                    'https://api.icedatavault.icedataservices.com/v2/data/2021/02/12/S207/'
                    'WATCHLIST/20210212-S207_WATCHLIST_username_0_0'
                ),
                'file_path': (
                    '/home/jacopo/Mkt_Data/2021/02/12/S207/WATCHLIST/WATCHLIST_207_20210212.txt.bz2'
                ),
                'source_id': 207,
                'reference_date': '2021-02-12T00:00:00',
                'size': 93624504,
                'md5sum': 'a8edc2d1c5ed49881f7bb238631b5000',
            },
        ]
        path_to_manifest_file = pathlib.Path(__file__).resolve().parent.joinpath(
            'static_data', 'download_manifest_20210212.json',
        ).as_posix()
        with open(path_to_manifest_file, 'w') as outfile:
            json.dump(existing_manifest_content, outfile, indent=2)
        file_payload = [
            ItemToDownload(
                file_name="WATCHLIST_367_20200212.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/"
                    "WATCHLIST/20200716-S367_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data", "2020", "02", "12", "S367", "WATCHLIST",
                    "WATCHLIST_367_20200212.txt.bz2"
                ).as_posix(),
                source_id=367,
                reference_date="2020-02-12T00:00:00",
                size=100145874,
                md5sum="fb34325ec9262adc74c945a9e7c9b465",
            ),
        ]
        # Exercise
        pdp.write_manifest_to_json(file_payload, path_to_manifest_file)
        # Verify
        expected_file_content = existing_manifest_content.copy()
        expected_file_content.extend(file_payload)
        with open(path_to_manifest_file, 'r') as infile:
            manifest_content = json.load(infile)
        assert manifest_content == expected_file_content
        # Cleanup - none
        pathlib.Path(path_to_manifest_file).unlink()


class TestUpdateManifestFile:
    def test_update_of_existing_manifest_file(self):
        """Tests the update of an existing download manifest file with additional data.
        """
        # Setup
        existing_manifest_content = [
            {
                'file_name': 'WATCHLIST_207_20210212.txt.bz2',
                'download_url': (
                    'https://api.icedatavault.icedataservices.com/v2/data/2021/02/12/S207/'
                    'WATCHLIST/20210212-S207_WATCHLIST_username_0_0'
                ),
                'file_path': (
                    '/home/jacopo/Mkt_Data/2021/02/12/S207/WATCHLIST/WATCHLIST_207_20210212.txt.bz2'
                ),
                'source_id': 207,
                'reference_date': '2021-02-12T00:00:00',
                'size': 93624504,
                'md5sum': 'a8edc2d1c5ed49881f7bb238631b5000',
            },
            {
                'file_name': 'CROSSREF_207_20210212.txt.bz2',
                'download_url': (
                    'https://api.icedatavault.icedataservices.com/v2/data/2021/02/12/S207/'
                    'CROSS/20210212-S207_CROSS_ALL_0_0'
                ),
                'file_path': (
                    '/home/jacopo/Mkt_Data/2021/02/12/S207/CROSS/CROSSREF_207_20210212.txt.bz2'
                ),
                'source_id': 207,
                'reference_date': '2021-02-12T00:00:00',
                'size': 13446060,
                'md5sum': '9af83565158f62920f9055c5ef29c335',
            },
            {
                'file_name': 'COREREF_207_20210212.txt.bz2',
                'download_url': (
                    'https://api.icedatavault.icedataservices.com/v2/data/2021/02/12/S207/'
                    'CORE/20210212-S207_CORE_ALL_0_0'
                ),
                'file_path': (
                    '/home/jacopo/Mkt_Data/2021/02/12/S207/CORE/COREREF_207_20210212.txt.bz2'
                ),
                'source_id': 207,
                'reference_date': '2021-02-12T00:00:00',
                'size': 4204727,
                'md5sum': 'db66eacc4354b667080f2d2178b45c32',
            }
        ]
        manifest_update = [
            ItemToDownload(
                file_name="WATCHLIST_367_20200212.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/"
                    "WATCHLIST/20200716-S367_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data", "2020", "02", "12", "S367", "WATCHLIST",
                    "WATCHLIST_367_20200212.txt.bz2"
                ).as_posix(),
                source_id=367,
                reference_date="2020-02-12T00:00:00",
                size=100145874,
                md5sum="fb34325ec9262adc74c945a9e7c9b465",
            ),
        ]
        path_to_manifest_file = pathlib.Path(__file__).resolve().parent.joinpath(
            'static_data', 'download_manifest_20210212.json',
        ).as_posix()
        with open(path_to_manifest_file, 'w') as outfile:
            json.dump(existing_manifest_content, outfile, indent=2)

        # Exercise
        pdp.update_manifest_file(path_to_manifest_file, manifest_update)
        # Verify
        with open(path_to_manifest_file, 'r') as infile:
            updated_file_content = json.load(infile)
        expected_file_content = existing_manifest_content.copy()
        expected_file_content.extend(manifest_update)
        assert updated_file_content == expected_file_content
        # Cleanup - none
        pathlib.Path(path_to_manifest_file).unlink()

    def test_handling_of_existing_information(self):
        """Checks that information already present in the manifest file is not re-added."""
        # Setup
        existing_manifest_content = [
            {
                'file_name': 'WATCHLIST_207_20210212.txt.bz2',
                'download_url': (
                    'https://api.icedatavault.icedataservices.com/v2/data/2021/02/12/S207/'
                    'WATCHLIST/20210212-S207_WATCHLIST_username_0_0'
                ),
                'file_path': (
                    '/home/jacopo/Mkt_Data/2021/02/12/S207/WATCHLIST/WATCHLIST_207_20210212.txt.bz2'
                ),
                'source_id': 207,
                'reference_date': '2021-02-12T00:00:00',
                'size': 93624504,
                'md5sum': 'a8edc2d1c5ed49881f7bb238631b5000',
            },
            {
                'file_name': 'CROSSREF_207_20210212.txt.bz2',
                'download_url': (
                    'https://api.icedatavault.icedataservices.com/v2/data/2021/02/12/S207/'
                    'CROSS/20210212-S207_CROSS_ALL_0_0'
                ),
                'file_path': (
                    '/home/jacopo/Mkt_Data/2021/02/12/S207/CROSS/CROSSREF_207_20210212.txt.bz2'
                ),
                'source_id': 207,
                'reference_date': '2021-02-12T00:00:00',
                'size': 13446060,
                'md5sum': '9af83565158f62920f9055c5ef29c335',
            },
            {
                'file_name': 'COREREF_207_20210212.txt.bz2',
                'download_url': (
                    'https://api.icedatavault.icedataservices.com/v2/data/2021/02/12/S207/'
                    'CORE/20210212-S207_CORE_ALL_0_0'
                ),
                'file_path': (
                    '/home/jacopo/Mkt_Data/2021/02/12/S207/CORE/COREREF_207_20210212.txt.bz2'
                ),
                'source_id': 207,
                'reference_date': '2021-02-12T00:00:00',
                'size': 4204727,
                'md5sum': 'db66eacc4354b667080f2d2178b45c32',
            }
        ]
        manifest_update = [
            ItemToDownload(
                file_name='WATCHLIST_207_20210212.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2021/02/12/S207/'
                    'WATCHLIST/20210212-S207_WATCHLIST_username_0_0'
                ),
                file_path=(
                    '/home/jacopo/Mkt_Data/2021/02/12/S207/WATCHLIST/WATCHLIST_207_20210212.txt.bz2'
                ),
                source_id=207,
                reference_date='2021-02-12T00:00:00',
                size=93624504,
                md5sum='a8edc2d1c5ed49881f7bb238631b5000',
            ),
        ]
        path_to_manifest_file = pathlib.Path(__file__).resolve().parent.joinpath(
            'static_data', 'download_manifest_20210212.json',
        ).as_posix()
        with open(path_to_manifest_file, 'w') as outfile:
            json.dump(existing_manifest_content, outfile, indent=2)

        # Exercise
        pdp.update_manifest_file(path_to_manifest_file, manifest_update)
        # Verify
        with open(path_to_manifest_file, 'r') as infile:
            updated_file_content = json.load(infile)
        expected_file_content = existing_manifest_content.copy()
        assert updated_file_content == expected_file_content
        # Cleanup - none
        pathlib.Path(path_to_manifest_file).unlink()


class TestGenerateManifestFile:
    def test_generation_of_manifest_file(
        self,
        mocked_download_info_single_source_multiple_days_concurrent,
    ):
        # Setup - none
        # Exercise
        pdp.generate_manifest_file(mocked_download_info_single_source_multiple_days_concurrent)
        # Verify
        expected_file_content_20200717 = [
            {
                "file_name": "COREREF_207_20200717.txt.bz2",
                "download_url": (
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/17/S207/CORE/"
                    "20200717-S207_CORE_ALL_0_0"
                ),
                "file_path": pathlib.Path(__file__).resolve().parent.joinpath(
                    "Temp/Data/2020/07/17/S207/CORE/COREREF_207_20200717.txt.bz2"
                ).as_posix(),
                "source_id": 207,
                "reference_date": "2020-07-17T00:00:00",
                "size": 3910430,
                "md5sum": "63958e5bc651b95da410e76a1763dde7"
            },
            {
                "file_name": "CROSSREF_207_20200717.txt.bz2",
                "download_url": (
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/17/S207/CROSS/"
                    "20200717-S207_CROSS_ALL_0_0"
                ),
                "file_path": pathlib.Path(__file__).resolve().parent.joinpath(
                             "Temp/Data/2020/07/17/S207/CROSS/CROSSREF_207_20200717.txt.bz2"
                ).as_posix(),
                "source_id": 207,
                "reference_date": "2020-07-17T00:00:00",
                "size": 13816558,
                "md5sum": "d1316740714e9b13cf03acf02a23c596"
            },
            {
                "file_name": "WATCHLIST_207_20200717.txt.bz2",
                "download_url": (
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/17/S207/"
                    "WATCHLIST/20200717-S207_WATCHLIST_username_0_0"
                ),
                "file_path": pathlib.Path(__file__).resolve().parent.joinpath(
                    "Temp/Data/2020/07/17/S207/WATCHLIST/WATCHLIST_207_20200717.txt.bz2"
                ).as_posix(),
                "source_id": 207,
                "reference_date": "2020-07-17T00:00:00",
                "size": 63958346,
                "md5sum": "9be9099186dfd8a7e0012e58fd49a3da"
            },
        ]
        expected_file_content_20200720 = [
            {
                "file_name": "COREREF_207_20200720.txt.bz2",
                "download_url": (
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/CORE/"
                    "20200720-S207_CORE_ALL_0_0"
                ),
                "file_path": pathlib.Path(__file__).resolve().parent.joinpath(
                    "Temp/Data/2020/07/20/S207/CORE/COREREF_207_20200720.txt.bz2"
                ).as_posix(),
                "source_id": 207,
                "reference_date": "2020-07-20T00:00:00",
                "size": 4548016,
                "md5sum": "a46a5f07b6a402d4023ef550df6a12e4"
            },
            {
                "file_name": "CROSSREF_207_20200720.txt.bz2",
                "download_url": (
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/CROSS/"
                    "20200720-S207_CROSS_ALL_0_0"
                ),
                "file_path": pathlib.Path(__file__).resolve().parent.joinpath(
                    "Temp/Data/2020/07/20/S207/CROSS/CROSSREF_207_20200720.txt.bz2"
                ).as_posix(),
                "source_id": 207,
                "reference_date": "2020-07-20T00:00:00",
                "size": 14571417,
                "md5sum": "6b3dbd152e7dccf4147f62b6ce1c78c3"
            },
            {
                "file_name": "WATCHLIST_207_20200720.txt.bz2",
                "download_url": (
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/"
                    "WATCHLIST/20200720-S207_WATCHLIST_username_0_0"
                ),
                "file_path": pathlib.Path(__file__).resolve().parent.joinpath(
                    "Temp/Data/2020/07/20/S207/WATCHLIST/WATCHLIST_207_20200720.txt.bz2"
                ).as_posix(),
                "source_id": 207,
                "reference_date": "2020-07-20T00:00:00",
                "size": 70613654,
                "md5sum": "ba2c00511520a3cf4b5383ceedb3b41d"
            },
        ]
        with pathlib.Path(__file__).resolve().parent.joinpath(
            "Temp/Data/2020/07/17/download_manifest_20200717.json"
        ).open("r") as infile:
            download_manifest_20200717_content = json.load(infile)
            assert download_manifest_20200717_content == expected_file_content_20200717

        with pathlib.Path(__file__).resolve().parent.joinpath(
            "Temp/Data/2020/07/20/download_manifest_20200720.json"
        ).open("r") as infile:
            download_manifest_20200720_content = json.load(infile)
            assert download_manifest_20200720_content == expected_file_content_20200720
        # Cleanup
        pathlib.Path(__file__).resolve().parent.joinpath(
            "Temp/Data/2020/07/17/download_manifest_20200717.json"
        ).unlink()
        pathlib.Path(__file__).resolve().parent.joinpath(
            "Temp/Data/2020/07/20/download_manifest_20200720.json"
        ).unlink()
        directory_root = pathlib.Path(__file__).resolve().parent / "Temp"
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()


class TestPreSynchronousDownloadProcessor:
    def test_pre_synchronous_download_data_processing(
        self,
        mocked_files_available_to_download_single_source_single_day
    ):
        # Setup
        discovered_files_info = mocked_files_available_to_download_single_source_single_day
        path_to_data_directory = pathlib.Path(__file__).resolve().parent.joinpath("Temp/Data")
        # Exercise
        download_details = pdp.pre_synchronous_download_processor(
            discovered_files_info,
            path_to_data_directory,
        )
        # Verify
        expected_download_details = [
            DownloadDetails(
                file_name="COREREF_945_20200722.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/CORE/"
                    "20200722-S945_CORE_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Temp/Data/2020/07/22/S945/CORE", "COREREF_945_20200722.txt.bz2"
                ),
                source_id=945,
                reference_date=datetime.datetime(year=2020, month=7, day=22),
                size=17734,
                md5sum="3548e03c8833b0e2133c80ac3b1dcdac",
                is_partitioned=None,
            ),
            DownloadDetails(
                file_name="CROSSREF_945_20200722.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/CROSS/"
                    "20200722-S945_CROSS_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Temp/Data/2020/07/22/S945/CROSS", "CROSSREF_945_20200722.txt.bz2"
                ),
                source_id=945,
                reference_date=datetime.datetime(year=2020, month=7, day=22),
                size=32822,
                md5sum="936c0515dcbc27d2e2fc3ebdcf5f883a",
                is_partitioned=None,
            ),
            DownloadDetails(
                file_name="WATCHLIST_945_20200722.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/"
                    "WATCHLIST/20200722-S945_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Temp/Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722.txt.bz2"
                ),
                source_id=945,
                reference_date=datetime.datetime(year=2020, month=7, day=22),
                size=61663360,
                md5sum="78571e930fb12fcfb2fb70feb07c7bcf",
                is_partitioned=None,
            ),
        ]
        expected_content_of_manifest_file = [
            ItemToDownload(
                file_name="COREREF_945_20200722.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/CORE/"
                    "20200722-S945_CORE_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Temp/Data/2020/07/22/S945/CORE", "COREREF_945_20200722.txt.bz2"
                ).as_posix(),
                source_id=945,
                reference_date="2020-07-22T00:00:00",
                size=17734,
                md5sum="3548e03c8833b0e2133c80ac3b1dcdac",
            ),
            ItemToDownload(
                file_name="CROSSREF_945_20200722.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/CROSS/"
                    "20200722-S945_CROSS_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Temp/Data/2020/07/22/S945/CROSS", "CROSSREF_945_20200722.txt.bz2"
                ).as_posix(),
                source_id=945,
                reference_date="2020-07-22T00:00:00",
                size=32822,
                md5sum="936c0515dcbc27d2e2fc3ebdcf5f883a",
            ),
            ItemToDownload(
                file_name="WATCHLIST_945_20200722.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/"
                    "WATCHLIST/20200722-S945_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Temp/Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722.txt.bz2"
                ).as_posix(),
                source_id=945,
                reference_date="2020-07-22T00:00:00",
                size=61663360,
                md5sum="78571e930fb12fcfb2fb70feb07c7bcf",
            ),
        ]
        assert download_details == expected_download_details
        with pathlib.Path(__file__).resolve().parent.joinpath(
            "Temp/Data/2020/07/22/download_manifest_20200722.json"
        ).open("r") as infile:
            manifest_file_content = json.load(infile)
        assert manifest_file_content == expected_content_of_manifest_file
        # Cleanup - none
        pathlib.Path(__file__).resolve().parent.joinpath(
            "Temp/Data/2020/07/22/download_manifest_20200722.json"
        ).unlink()
        directory_root = pathlib.Path(__file__).resolve().parent / "Temp"
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()

##########################################################################################


class TestCalculateNumberOfSameSizePartitions:
    @pytest.mark.parametrize(
        "file_size_in_bytes, partition_size_in_mib, correct_number_of_same_size_partitions",
        [(57671680, 5.0, 11), (100145874, 4.2, 22)],
    )
    def test_calculation_of_number_of_same_size_partitions(
        self,
        file_size_in_bytes,
        partition_size_in_mib,
        correct_number_of_same_size_partitions,
    ):
        # Setup - none
        # Exercise
        calculated_number_of_same_size_partitions = pdp.calculate_number_of_same_size_partitions(
            file_size_in_bytes, partition_size_in_mib
        )
        # Verify
        assert (
            calculated_number_of_same_size_partitions
            == correct_number_of_same_size_partitions
        )
        # Cleanup - none


class TestCalculateSizeOfLastPartition:
    @pytest.mark.parametrize(
        "file_size_in_bytes, partition_size_in_mib, correct_size_of_last_partition",
        [(57671680, 5.0, 0), (100145874, 4.7, 1579734)],
    )
    def test_calculation_size_of_last_partition(
        self, file_size_in_bytes, partition_size_in_mib, correct_size_of_last_partition
    ):
        # Setup - none
        # Exercise
        calculated_size_of_last_partition = pdp.calculate_size_of_last_partition(
            file_size_in_bytes, partition_size_in_mib,
        )
        # Verify
        assert calculated_size_of_last_partition == correct_size_of_last_partition
        # Cleanup - none


class TestCalculateListOfPartitionUpperExtremities:
    def test_calculation_of_upper_extremities(self):
        # Setup
        file_size = 61663360
        partition_size_in_mib = 5.0
        # Exercise
        calculated_list_of_upper_extremities = pdp.calculate_list_of_partition_upper_extremities(
            file_size, partition_size_in_mib
        )
        # Verify
        correct_list_of_upper_extremities = [
            5242880,
            10485760,
            15728640,
            20971520,
            26214400,
            31457280,
            36700160,
            41943040,
            47185920,
            52428800,
            57671680,
            61663360,
        ]
        assert calculated_list_of_upper_extremities == correct_list_of_upper_extremities
        # Cleanup - none

    def test_calculation_of_upper_extremities_when_file_size_is_multiple_of_partition_size(
        self,
    ):
        # Setup
        file_size = 57671680
        partition_size_in_mib = 5.0
        # Exercise
        calculated_list_of_upper_extremities = pdp.calculate_list_of_partition_upper_extremities(
            file_size, partition_size_in_mib
        )
        # Verify
        correct_list_of_upper_extremities = [
            5242880,
            10485760,
            15728640,
            20971520,
            26214400,
            31457280,
            36700160,
            41943040,
            47185920,
            52428800,
            57671680,
        ]
        assert calculated_list_of_upper_extremities == correct_list_of_upper_extremities
        # Cleanup - none


class TestCalculateListOfPartitionLowerExtremities:
    def test_calculation_of_lower_extremities(self):
        # Setup
        file_size = 61663360
        partition_size_in_mib = 5.0
        # Exercise
        calculated_list_of_upper_extremities = pdp.calculate_list_of_partition_lower_extremities(
            file_size, partition_size_in_mib
        )
        # Verify
        correct_list_of_upper_extremities = [
            0,
            5242881,
            10485761,
            15728641,
            20971521,
            26214401,
            31457281,
            36700161,
            41943041,
            47185921,
            52428801,
            57671681,
        ]
        assert calculated_list_of_upper_extremities == correct_list_of_upper_extremities
        # Cleanup - none

    def test_calculation_of_lower_extremities_when_file_size_is_multiple_of_partition_size(
        self,
    ):
        # Setup
        file_size = 57671680
        partition_size_in_mib = 5.0
        # Exercise
        calculated_list_of_upper_extremities = pdp.calculate_list_of_partition_lower_extremities(
            file_size, partition_size_in_mib
        )
        # Verify
        correct_list_of_upper_extremities = [
            0,
            5242881,
            10485761,
            15728641,
            20971521,
            26214401,
            31457281,
            36700161,
            41943041,
            47185921,
            52428801,
        ]
        assert calculated_list_of_upper_extremities == correct_list_of_upper_extremities
        # Cleanup - none


class TestCalculateListOfPartitionExtremities:
    def test_calculation_of_partition_extremities(self):
        # Setup
        file_size = 61663360
        size_of_partition_in_mib = 5.0
        # Exercise
        calculated_partition_extremities = pdp.calculate_list_of_partition_extremities(
            file_size, size_of_partition_in_mib
        )
        # Verify
        correct_partition_extremities = [
            {"start": 0, "end": 5242880},
            {"start": 5242881, "end": 10485760},
            {"start": 10485761, "end": 15728640},
            {"start": 15728641, "end": 20971520},
            {"start": 20971521, "end": 26214400},
            {"start": 26214401, "end": 31457280},
            {"start": 31457281, "end": 36700160},
            {"start": 36700161, "end": 41943040},
            {"start": 41943041, "end": 47185920},
            {"start": 47185921, "end": 52428800},
            {"start": 52428801, "end": 57671680},
            {"start": 57671681, "end": 61663360},
        ]
        assert calculated_partition_extremities == correct_partition_extremities
        # Cleanup - none

    def test_calculation_of_partition_extremities_file_size_multiple_of_partition_size(
        self,
    ):
        # Setup
        file_size = 57671680
        size_of_partition_in_mib = 5.0
        # Exercise
        calculated_partition_extremities = pdp.calculate_list_of_partition_extremities(
            file_size, size_of_partition_in_mib
        )
        # Verify
        correct_partition_extremities = [
            {"start": 0, "end": 5242880},
            {"start": 5242881, "end": 10485760},
            {"start": 10485761, "end": 15728640},
            {"start": 15728641, "end": 20971520},
            {"start": 20971521, "end": 26214400},
            {"start": 26214401, "end": 31457280},
            {"start": 31457281, "end": 36700160},
            {"start": 36700161, "end": 41943040},
            {"start": 41943041, "end": 47185920},
            {"start": 47185921, "end": 52428800},
            {"start": 52428801, "end": 57671680},
        ]
        assert calculated_partition_extremities == correct_partition_extremities
        # Cleanup - none


class TestFormatQueryString:
    @pytest.mark.parametrize(
        "parameters_to_encode, correct_query_string",
        [
            ({"start": 0, "end": 943718}, "start=0&end=943718"),
            ({"start": 9437181, "end": 10380898}, "start=9437181&end=10380898"),
            ({"start": 24536669, "end": 25217299}, "start=24536669&end=25217299"),
        ],
    )
    def test_generation_of_query_string(
        self, parameters_to_encode, correct_query_string
    ):
        # Setup - none
        # Exercise
        generated_query_string = pdp.format_query_string(
            parameters_to_encode
        )
        # Verify
        assert generated_query_string == correct_query_string
        # Cleanup - none


class TestJoinBaseUrlAndQueryString:
    @pytest.mark.parametrize(
        "query_string, correct_encoded_url",
        [
            (
                "start=0&end=943718",
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
                "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0?start=0&end=943718",
            ),
            (
                "start=9437181&end=10380898",
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
                "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0?start=9437181&end=10380898",
            ),
            (
                "start=24536669&end=25217299",
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
                "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0?start=24536669&end=25217299",
            ),
        ],
    )
    def test_generation_of_url_with_query_string(
        self, query_string, correct_encoded_url
    ):
        # Setup
        base_url = (
            "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
            "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0"
        )
        # Exercise
        generated_url = pdp.join_base_url_and_query_string(
            base_url, query_string
        )
        # Verify
        assert generated_url == correct_encoded_url
        # Cleanup - none


class TestGeneratePartitionDownloadUrl:
    @pytest.mark.parametrize(
        "partition_extremities, correct_partition_download_url",
        [
            (
                {"start": 0, "end": 943718}, (
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
                    "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0?start=0&end=943718"
                ),
            ),
            (
                {"start": 9437181, "end": 10380898}, (
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
                    "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0?start=9437181&end=10380898"
                ),
            ),
            (
                {"start": 24536669, "end": 25217299}, (
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
                    "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0"
                    "?start=24536669&end=25217299"
                ),
            ),
        ],
    )
    def test_generation_of_partition_download_url(
        self, partition_extremities, correct_partition_download_url
    ):
        # Setup
        whole_file_download_url = (
            "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
            "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0"
        )
        # Exercise
        generated_partition_download_url = pdp.create_partition_download_url(
            whole_file_download_url, partition_extremities
        )
        # Verify
        assert generated_partition_download_url == correct_partition_download_url
        # Cleanup - none

    def test_generation_of_partition_url_when_base_url_has_trailing_slash(self):
        # Setup
        whole_file_download_url = (
            "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
            "S905/WATCHLIST/20200722-S905_WATCHLIST_username_0_0/"
        )
        partition_extremities = {"start": 24536669, "end": 25217299}
        # Exercise
        generated_partition_download_url = pdp.create_partition_download_url(
            whole_file_download_url, partition_extremities
        )
        # Verify
        correct_partition_download_url = (
            "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/"
            "S905/WATCHLIST/"
            "20200722-S905_WATCHLIST_username_0_0?start=24536669&end=25217299"
        )
        assert generated_partition_download_url == correct_partition_download_url
        # Cleanup - none


class TestGeneratePathToFilePartition:
    @pytest.mark.parametrize(
        "partition_index, correct_path_to_file_partition",
        [
            (
                0,
                pathlib.Path(__file__).resolve().parent
                / "Data"
                / "2020"
                / "07"
                / "22"
                / "S905"
                / "WATCHLIST"
                / "WATCHLIST_905_20200722_1.txt",
            ),
            (
                1,
                pathlib.Path(__file__).resolve().parent
                / "Data"
                / "2020"
                / "07"
                / "22"
                / "S905"
                / "WATCHLIST"
                / "WATCHLIST_905_20200722_2.txt",
            ),
            (
                32,
                pathlib.Path(__file__).resolve().parent
                / "Data"
                / "2020"
                / "07"
                / "22"
                / "S905"
                / "WATCHLIST"
                / "WATCHLIST_905_20200722_33.txt",
            ),
        ],
    )
    def test_generation_of_path_to_file_partition(
        self, partition_index, correct_path_to_file_partition
    ):
        # Setup
        path_to_file = (
            pathlib.Path(__file__).resolve().parent
            / "Data"
            / "2020"
            / "07"
            / "22"
            / "S905"
            / "WATCHLIST"
            / "WATCHLIST_905_20200722.txt.bz2"
        )
        # Exercise
        generated_path_to_file_partition = pdp.generate_path_to_file_partition(
            path_to_file, partition_index
        )
        # Verify
        assert generated_path_to_file_partition == correct_path_to_file_partition
        # Cleanup - none


class TestCreateListOfFileSpecificPartitionsDownloadInfo:
    def test_generation_list_of_partitions_download_info(
        self,
        mocked_download_details_single_instrument,
        mocked_file_partitions_single_instrument,
    ):
        # Setup
        file_specific_download_details = mocked_download_details_single_instrument
        partition_size_in_mb = 5.0
        # Exercise
        partitions_download_info = pdp.create_list_of_file_specific_partition_download_info(
            file_specific_download_details, partition_size_in_mb
        )
        # Verify
        assert partitions_download_info == mocked_file_partitions_single_instrument
        # Cleanup - none


class TestFilterFilesToSplit:
    def test_filtering_of_files_to_split(
        self, mocked_download_details_multiple_sources_single_day
    ):
        # Setup
        whole_files_download_details = (
            mocked_download_details_multiple_sources_single_day
        )
        # Exercise
        files_to_partition = pdp.filter_files_to_split(
            whole_files_download_details
        )
        # Verify
        expected_files_to_partition = [
            DownloadDetails(
                file_name="CROSSREF_207_20200721.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/CROSS/"
                    "20200721-S207_CROSS_ALL_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data", "2020", "07", "21", "S207", "CROSS", "CROSSREF_207_20200721.txt.bz2",
                ),
                source_id=207,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=14690557,
                md5sum="f2683cd87a7b29f3b8776373d56a8456",
                is_partitioned=True,
            ),
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
            ),
            DownloadDetails(
                file_name="WATCHLIST_367_20200721.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/"
                    "WATCHLIST/20200721-S367_WATCHLIST_username_0_0"
                ),
                file_path=pathlib.Path(__file__).resolve().parent.joinpath(
                    "Data", "2020", "07", "21", "S367", "WATCHLIST",
                    "WATCHLIST_367_20200721.txt.bz2",
                ),
                source_id=367,
                reference_date=datetime.datetime(year=2020, month=7, day=21),
                size=82451354,
                md5sum="62df718ef5eb5f9f1ea3f6ea1f826c30",
                is_partitioned=True,
            ),
        ]
        assert files_to_partition == expected_files_to_partition
        # Cleanup - none


class TestGenerateWholeFilesDownloadManifest:
    def test_generation_of_whole_files_download_manifest(
        self,
        mocked_whole_files_download_details_single_source_single_day,
    ):
        # Setup - none
        # Exercise
        download_manifest = pdp.generate_whole_files_download_manifest(
            mocked_whole_files_download_details_single_source_single_day,
        )
        # Verify
        expected_download_manifest = [
            file for file in mocked_whole_files_download_details_single_source_single_day
            if file.is_partitioned is False
        ]
        assert download_manifest == expected_download_manifest
        # Cleanup - none

    def test_generation_of_whole_files_download_manifest_with_no_whole_files(
        self,
        mocked_whole_files_download_details_single_source_single_day,
    ):
        # Setup
        download_info = [
            file for file in mocked_whole_files_download_details_single_source_single_day
            if file.is_partitioned is True
        ]
        # Exercise
        download_manifest = pdp.generate_whole_files_download_manifest(download_info)
        # Verify
        assert download_manifest == []
        # Cleanup - none


class TestGeneratePartitionsDownloadManifest:
    def test_generation_of_partitions_download_manifest(
        self,
        mocked_whole_files_download_details_single_source_single_day,
        mocked_partitions_download_details_single_source_single_day,
    ):
        # Setup
        partition_size = 5.0
        # Exercise
        download_manifest = pdp.generate_partitions_download_manifest(
            mocked_whole_files_download_details_single_source_single_day,
            partition_size_in_mib=partition_size,
        )
        # Verify
        assert download_manifest == mocked_partitions_download_details_single_source_single_day
        # Cleanup - none

    def test_generation_of_partitions_download_manifest_with_no_partitioned_files(
        self,
        mocked_whole_files_download_details_single_source_single_day,
        mocked_partitions_download_details_single_source_single_day,
    ):
        # Setup
        partition_size = 5.0
        whole_files_download_info = [
            file for file in mocked_whole_files_download_details_single_source_single_day
            if file.is_partitioned is False
        ]
        # Exercise
        download_manifest = pdp.generate_partitions_download_manifest(
            whole_files_download_info,
            partition_size_in_mib=partition_size,
        )
        # Verify
        assert download_manifest == []
        # Cleanup - none


class TestPreConcurrentDownloadProcessor:
    def test_pre_concurrent_download_data_processing(
        self,
        mocked_files_available_to_download_single_source_single_day,
        mocked_partitions_download_details_single_source_single_day,
        mocked_whole_files_download_details_single_source_single_day,
    ):
        # Setup
        discovered_files_info = mocked_files_available_to_download_single_source_single_day
        path_to_data_directory = pathlib.Path(__file__).resolve().parent.joinpath("Data").as_posix()
        # Exercise
        download_manifest = pdp.pre_concurrent_download_processor(
            discovered_files_info,
            path_to_data_directory,
        )
        # Verify
        expected_reference_data = mocked_whole_files_download_details_single_source_single_day
        expected_whole_files = [
            file for file in expected_reference_data if file.is_partitioned is False
        ]
        expected_partitions = mocked_partitions_download_details_single_source_single_day
        assert download_manifest.files_reference_data == expected_reference_data
        assert download_manifest.whole_files_to_download == expected_whole_files
        assert download_manifest.partitions_to_download == expected_partitions
        # Cleanup
        pathlib.Path(__file__).resolve().parent.joinpath(
            "Data/2020/07/22/download_manifest_20200722.json"
        ).unlink()
        directory_root = pathlib.Path(__file__).resolve().parent / "Data"
        for directory in list(directory_root.glob('**/'))[::-1]:
            directory.rmdir()
