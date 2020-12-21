import datetime
from pathlib import Path

import pytest
import responses

from datavault_api_client.data_structures import (
    ConcurrentDownloadManifest,
    DiscoveredFileInfo,
    DownloadDetails,
    PartitionDownloadDetails,
)


@pytest.fixture
def mocked_response():
    """A pytest fixture to mock the behaviour of a server sending back a response."""
    with responses.RequestsMock() as resp:
        yield resp


@pytest.fixture
def mocked_top_level_datavault_api(mocked_response):
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list",
        json=[
            {
                'name': '2020',
                'parent': '/v2/list',
                'url': '/v2/list/2020',
                'size': 0,
                'createdAt': '2020-01-01T00:00:00',
                'updatedAt': '2020-12-01T00:00:00',
                'writable': False,
                'directory': True
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            'Date': 'Tue, 01 Dec 2020 16:49:36 GMT',
            'Transfer-Encoding': 'chunked',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, DELETE, PUT',
            'Access-Control-Max-Age': '3600',
            'Access-Control-Allow-Headers': 'x-request-with, authorization, content-type',
            'Access-Control-Allow-Credentials': 'true',
            'Access-Control-Expose-Headers': (
                'Cache-Control, Content-Language, Content-Length, Content-Type, '
                'Expires, Last-Modified, Pragma'
            ),
            'X-Content-Type-Options': 'nosniff',
            'X-XSS-Protection': '1; mode=block',
            'Cache-Control': 'no-cache, no-store, max-age=0, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
            'Strict-Transport-Security': 'max-age=31536000 ; includeSubDomains',
            'X-Frame-Options': 'DENY',
        },
    )


@pytest.fixture
def mocked_top_level_datavault_api_failed_request(mocked_response):
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list",
        json=[
            {
                'error': 'ClientError',
            }
        ],
        status=400,
        content_type="application/json;charset=UTF-8",
        headers={
            'Date': 'Tue, 01 Dec 2020 16:49:36 GMT',
            'Transfer-Encoding': 'chunked',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, DELETE, PUT',
            'Access-Control-Max-Age': '3600',
            'Access-Control-Allow-Headers': 'x-request-with, authorization, content-type',
            'Access-Control-Allow-Credentials': 'true',
            'Access-Control-Expose-Headers': (
                'Cache-Control, Content-Language, Content-Length, Content-Type, '
                'Expires, Last-Modified, Pragma'
            ),
            'X-Content-Type-Options': 'nosniff',
            'X-XSS-Protection': '1; mode=block',
            'Cache-Control': 'no-cache, no-store, max-age=0, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
            'Strict-Transport-Security': 'max-age=31536000 ; includeSubDomains',
            'X-Frame-Options': 'DENY',
        },
    )


@pytest.fixture
def mocked_datavault_api_with_down_the_line_failed_request(mocked_response):
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020",
        json=[
            {
                'name': '12',
                'parent': '/v2/list/2020',
                'url': '/v2/list/2020/12',
                'size': 0,
                'createdAt': '2020-12-01T00:00:00',
                'updatedAt': '2020-12-02T00:00:00',
                'writable': False,
                'directory': True
            },
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            'Date': 'Wed, 02 Dec 2020 13:21:52 GMT',
            'Transfer-Encoding': 'chunked',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, DELETE, PUT',
            'Access-Control-Max-Age': '3600',
            'Access-Control-Allow-Headers': 'x-request-with, authorization, content-type',
            'Access-Control-Allow-Credentials': 'true',
            'Access-Control-Expose-Headers': (
                'Cache-Control, Content-Language, Content-Length, Content-Type, '
                'Expires, Last-Modified, Pragma'
            ),
            'X-Content-Type-Options': 'nosniff', 'X-XSS-Protection': '1; mode=block',
            'Cache-Control': 'no-cache, no-store, max-age=0, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0', 'Strict-Transport-Security': 'max-age=31536000 ; includeSubDomains',
            'X-Frame-Options': 'DENY',
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/12",
        json=[
            {
                'error': 'unauthorized',
                'error_description': 'Full authentication is required to access this resource',
            }
        ],
        status=401,
        content_type="application/json;charset=UTF-8",
        headers={
            'Date': 'Wed, 02 Dec 2020 13:24:50 GMT',
            'Transfer-Encoding': 'chunked',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, DELETE, PUT',
            'Access-Control-Max-Age': '3600',
            'Access-Control-Allow-Headers': 'x-request-with, authorization, content-type',
            'Access-Control-Allow-Credentials': 'true',
            'Access-Control-Expose-Headers': (
                'Cache-Control, Content-Language, Content-Length, Content-Type, '
                'Expires, Last-Modified, Pragma'
            ),
            'Cache-Control': 'no-store',
            'Pragma': 'no-cache',
            'WWW-Authenticate': (
                'Bearer realm="resource", error="unauthorized", '
                'error_description="Full authentication is required to access this resource"'
            ),
            'X-Content-Type-Options': 'nosniff',
            'X-XSS-Protection': '1; mode=block',
            'Strict-Transport-Security': 'max-age=31536000 ; includeSubDomains',
            'X-Frame-Options': 'DENY',
        },
    )


@pytest.fixture
def mocked_datavault_api_with_repeated_node(mocked_response):
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020",
        json=[
            {
                'name': '12',
                'parent': '/v2/list/2020',
                'url': '/v2/list/2020/12',
                'size': 0,
                'createdAt': '2020-12-01T00:00:00',
                'updatedAt': '2020-12-02T00:00:00',
                'writable': False,
                'directory': True
            },
            {
                'name': '12',
                'parent': '/v2/list/2020',
                'url': '/v2/list/2020/12',
                'size': 0,
                'createdAt': '2020-12-01T00:00:00',
                'updatedAt': '2020-12-02T00:00:00',
                'writable': False,
                'directory': True
            },
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            'Date': 'Wed, 02 Dec 2020 13:21:52 GMT',
            'Transfer-Encoding': 'chunked',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, DELETE, PUT',
            'Access-Control-Max-Age': '3600',
            'Access-Control-Allow-Headers': 'x-request-with, authorization, content-type',
            'Access-Control-Allow-Credentials': 'true',
            'Access-Control-Expose-Headers': (
                'Cache-Control, Content-Language, Content-Length, Content-Type, '
                'Expires, Last-Modified, Pragma'
            ),
            'X-Content-Type-Options': 'nosniff', 'X-XSS-Protection': '1; mode=block',
            'Cache-Control': 'no-cache, no-store, max-age=0, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0', 'Strict-Transport-Security': 'max-age=31536000 ; includeSubDomains',
            'X-Frame-Options': 'DENY',
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/12",
        json=[
            {
                'name': '01',
                'parent': '/v2/list/2020/12',
                'url': '/v2/list/2020/12/01',
                'size': 0,
                'createdAt': '2020-12-01T23:21:18',
                'updatedAt': '2020-12-02T09:14:31',
                'writable': False,
                'directory': True
            },
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            'Date': 'Wed, 02 Dec 2020 14:08:39 GMT',
            'Transfer-Encoding': 'chunked',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, DELETE, PUT',
            'Access-Control-Max-Age': '3600',
            'Access-Control-Allow-Headers': 'x-request-with, authorization, content-type',
            'Access-Control-Allow-Credentials': 'true',
            'Access-Control-Expose-Headers': (
                'Cache-Control, Content-Language, Content-Length, Content-Type, '
                'Expires, Last-Modified, Pragma'
            ),
            'X-Content-Type-Options': 'nosniff',
            'X-XSS-Protection': '1; mode=block',
            'Cache-Control': 'no-cache, no-store, max-age=0, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
            'Strict-Transport-Security': 'max-age=31536000 ; includeSubDomains',
            'X-Frame-Options': 'DENY',
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/12/01",
        json=[
            {
                'name': 'S945',
                'parent': '/v2/list/2020/12/01',
                'url': '/v2/list/2020/12/01/S945',
                'size': 0,
                'createdAt': '2020-12-01T23:10:48',
                'updatedAt': '2020-12-01T23:21:18',
                'writable': False,
                'directory': True
             },
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            'Date': 'Wed, 02 Dec 2020 14:16:28 GMT',
            'Transfer-Encoding': 'chunked',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, DELETE, PUT',
            'Access-Control-Max-Age': '3600',
            'Access-Control-Allow-Headers': 'x-request-with, authorization, content-type',
            'Access-Control-Allow-Credentials': 'true',
            'Access-Control-Expose-Headers': (
                'Cache-Control, Content-Language, Content-Length, Content-Type, '
                'Expires, Last-Modified, Pragma'
            ),
            'X-Content-Type-Options': 'nosniff',
            'X-XSS-Protection': '1; mode=block',
            'Cache-Control': 'no-cache, no-store, max-age=0, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
            'Strict-Transport-Security': 'max-age=31536000 ; includeSubDomains',
            'X-Frame-Options': 'DENY',
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/12/01/S945",
        json=[
            {
                'name': 'CORE',
                'parent': '/v2/list/2020/12/01/S945',
                'url': '/v2/list/2020/12/01/S945/CORE',
                'size': 0,
                'createdAt': '2020-12-01T23:10:48',
                'updatedAt': '2020-12-01T23:10:48',
                'writable': False,
                'directory': True
            },
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            'Date': 'Wed, 02 Dec 2020 14:18:35 GMT',
            'Transfer-Encoding': 'chunked',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, DELETE, PUT',
            'Access-Control-Max-Age': '3600',
            'Access-Control-Allow-Headers': 'x-request-with, authorization, content-type',
            'Access-Control-Allow-Credentials': 'true',
            'Access-Control-Expose-Headers': (
                'Cache-Control, Content-Language, Content-Length, Content-Type, '
                'Expires, Last-Modified, Pragma'
            ),
            'X-Content-Type-Options': 'nosniff',
            'X-XSS-Protection': '1; mode=block',
            'Cache-Control': 'no-cache, no-store, max-age=0, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
            'Strict-Transport-Security': 'max-age=31536000 ; includeSubDomains',
            'X-Frame-Options': 'DENY',
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/12/01/S945/CORE",
        json=[
            {
                'name': 'COREREF_945_20201201.txt.bz2',
                'fid': '20201201-S945_CORE_ALL_0_0',
                'parent': '/v2/list/2020/12/01/S945/CORE',
                'url': '/v2/data/2020/12/01/S945/CORE/20201201-S945_CORE_ALL_0_0',
                'size': 15680,
                'md5sum': 'c9cc20020def775933be0be9690a9b5a',
                'createdAt': '2020-12-01T23:10:48',
                'updatedAt': '2020-12-01T23:10:48',
                'writable': False,
                'directory': False,
            },
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            'Date': 'Wed, 02 Dec 2020 14:19:38 GMT',
            'Transfer-Encoding': 'chunked',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, DELETE, PUT',
            'Access-Control-Max-Age': '3600',
            'Access-Control-Allow-Headers': 'x-request-with, authorization, content-type',
            'Access-Control-Allow-Credentials': 'true',
            'Access-Control-Expose-Headers': (
                'Cache-Control, Content-Language, Content-Length, Content-Type, '
                'Expires, Last-Modified, Pragma'
            ),
            'X-Content-Type-Options': 'nosniff',
            'X-XSS-Protection': '1; mode=block',
            'Cache-Control': 'no-cache, no-store, max-age=0, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
            'Strict-Transport-Security': 'max-age=31536000 ; includeSubDomains',
            'X-Frame-Options': 'DENY',
        },
    )


"""Datavault API simulated at the instrument level."""


@pytest.fixture
def mocked_datavault_api_instrument_level(mocked_response):
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/16/S367/WATCHLIST",
        json=[
            {
                "name": "WATCHLIST_username_367_20200716.txt.bz2",
                "fid": "20200716-S367_WATCHLIST_username_0_0",
                "parent": "/v2/list/2020/07/16/S367/WATCHLIST",
                "url": "/v2/data/2020/07/16/S367/WATCHLIST/20200716-S367_WATCHLIST_username_0_0",
                "size": 100145874,
                "md5sum": "fb34325ec9262adc74c945a9e7c9b465",
                "createdAt": "2020-07-17T02:18:08",
                "updatedAt": "2020-07-17T02:18:08",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Thu, 30 Jul 2020 11:25:03 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization,"
            " content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers": "Cache-Control, Content-Language,"
            " Content-Length, Content-Type"
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )


@pytest.fixture
def mocked_files_available_to_download_single_instrument():
    files_available_to_download = [
        DiscoveredFileInfo(
            file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/"
                "07/16/S367/WATCHLIST/20200716-S367_WATCHLIST_username_0_0"
            ),
            source_id=367,
            reference_date=datetime.datetime(year=2020, month=7, day=16),
            size=100145874,
            md5sum="fb34325ec9262adc74c945a9e7c9b465",
        ),
    ]
    return files_available_to_download


@pytest.fixture
def mocked_download_details_single_instrument():
    download_details = DownloadDetails(
        file_name="WATCHLIST_367_20200716.txt.bz2",
        download_url=(
            "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/"
            "S367/WATCHLIST/20200716-S367_WATCHLIST_username_0_0"
        ),
        file_path=Path(__file__).resolve().parent.joinpath(
            "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716.txt.bz2"
        ),
        source_id=367,
        reference_date=datetime.datetime(year=2020, month=7, day=16),
        size=100145874,
        md5sum="fb34325ec9262adc74c945a9e7c9b465",
        is_partitioned=True,
    )
    return download_details


@pytest.fixture
def mocked_file_partitions_single_instrument():
    list_of_file_partitions = [
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=0&end=5242880"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_1.txt"
            ),
            partition_index=1,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=5242881&end=10485760"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_2.txt"
            ),
            partition_index=2,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=10485761&end=15728640"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_3.txt"
            ),
            partition_index=3,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=15728641&end=20971520"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_4.txt"
            ),
            partition_index=4,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=20971521&end=26214400"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_5.txt"
            ),
            partition_index=5,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=26214401&end=31457280"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_6.txt"
            ),
            partition_index=6,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=31457281&end=36700160"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_7.txt"
            ),
            partition_index=7,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=36700161&end=41943040"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_8.txt"
            ),
            partition_index=8,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=41943041&end=47185920"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_9.txt"
            ),
            partition_index=9,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=47185921&end=52428800"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_10.txt"
            ),
            partition_index=10,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=52428801&end=57671680"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_11.txt"
            ),
            partition_index=11,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=57671681&end=62914560"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_12.txt"
            ),
            partition_index=12,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=62914561&end=68157440"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_13.txt"
            ),
            partition_index=13,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=68157441&end=73400320"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_14.txt"
            ),
            partition_index=14,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=73400321&end=78643200"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_15.txt"
            ),
            partition_index=15,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=78643201&end=83886080"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_16.txt"
            ),
            partition_index=16,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=83886081&end=89128960"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_17.txt"
            ),
            partition_index=17,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=89128961&end=94371840"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_18.txt"
            ),
            partition_index=18,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=94371841&end=99614720"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_19.txt"
            ),
            partition_index=19,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200716.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/16/S367/WATCHLIST/"
                "20200716-S367_WATCHLIST_username_0_0?start=99614721&end=100145874"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/16/S367/WATCHLIST", "WATCHLIST_367_20200716_20.txt"
            ),
            partition_index=20,
        ),
    ]
    return list_of_file_partitions


"""Datavault API with single source and a single day."""


@pytest.fixture
def mocked_datavault_api_single_source_single_day(mocked_response):
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list",
        json=[
            {
                "name": "2020",
                "parent": "/v2/list",
                "url": "/v2/list/2020",
                "size": 0,
                "createdAt": "2020-01-01T00:00:00",
                "updatedAt": "2020-07-30T00:00:00",
                "writable": False,
                "directory": True,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Thu, 30 Jul 2020 11:19:56 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020",
        json=[
            {
                "name": "07",
                "parent": "/v2/list/2020",
                "url": "/v2/list/2020/07",
                "size": 0,
                "createdAt": "2020-07-01T00:00:00",
                "updatedAt": "2020-07-30T00:00:00",
                "writable": False,
                "directory": True,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Thu, 30 Jul 2020 11:20:44 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07",
        json=[
            {
                "name": "22",
                "parent": "/v2/list/2020/07",
                "url": "/v2/list/2020/07/22",
                "size": 0,
                "createdAt": "2020-07-22T22:44:01",
                "updatedAt": "2020-07-23T05:10:57",
                "writable": False,
                "directory": True,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Thu, 30 Jul 2020 11:22:42 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/22",
        json=[
            {
                "name": "S945",
                "parent": "/v2/list/2020/07/22",
                "url": "/v2/list/2020/07/22/S945",
                "size": 0,
                "createdAt": "2020-07-22T22:40:41",
                "updatedAt": "2020-07-22T22:44:01",
                "writable": False,
                "directory": True,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Thu, 30 Jul 2020 11:23:38 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/22/S945",
        json=[
            {
                "name": "CORE",
                "parent": "/v2/list/2020/07/22/S945",
                "url": "/v2/list/2020/07/22/S945/CORE",
                "size": 0,
                "createdAt": "2020-07-22T22:41:41",
                "updatedAt": "2020-07-22T22:41:41",
                "writable": False,
                "directory": True,
            },
            {
                "name": "CROSS",
                "parent": "/v2/list/2020/07/22/S945",
                "url": "/v2/list/2020/07/22/S945/CROSS",
                "size": 0,
                "createdAt": "2020-07-22T22:40:41",
                "updatedAt": "2020-07-22T22:40:41",
                "writable": False,
                "directory": True,
            },
            {
                "name": "WATCHLIST",
                "parent": "/v2/list/2020/07/22/S945",
                "url": "/v2/list/2020/07/22/S945/WATCHLIST",
                "size": 0,
                "createdAt": "2020-07-22T22:44:01",
                "updatedAt": "2020-07-22T22:44:01",
                "writable": False,
                "directory": True,
            },
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Thu, 30 Jul 2020 11:24:08 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/22/S945/WATCHLIST",
        json=[
            {
                "name": "WATCHLIST_username_945_20200722.txt.bz2",
                "fid": "20200722-S945_WATCHLIST_username_0_0",
                "parent": "/v2/list/2020/07/22/S945/WATCHLIST",
                "url": "/v2/data/2020/07/22/S945/WATCHLIST/20200722-S945_WATCHLIST_username_0_0",
                "size": 61663360,
                "md5sum": "78571e930fb12fcfb2fb70feb07c7bcf",
                "createdAt": "2020-07-22T22:44:01",
                "updatedAt": "2020-07-22T22:44:01",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Thu, 30 Jul 2020 11:25:04 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/22/S945/CORE",
        json=[
            {
                "name": "COREREF_945_20200722.txt.bz2",
                "fid": "20200722-S945_CORE_ALL_0_0",
                "parent": "/v2/list/2020/07/22/S945/CORE",
                "url": "/v2/data/2020/07/22/S945/CORE/20200722-S945_CORE_ALL_0_0",
                "size": 17734,
                "md5sum": "3548e03c8833b0e2133c80ac3b1dcdac",
                "createdAt": "2020-07-22T22:41:41",
                "updatedAt": "2020-07-22T22:41:41",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Thu, 30 Jul 2020 11:26:03 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/22/S945/CROSS",
        json=[
            {
                "name": "CROSSREF_945_20200722.txt.bz2",
                "fid": "20200722-S945_CROSS_ALL_0_0",
                "parent": "/v2/list/2020/07/22/S945/CROSS",
                "url": "/v2/data/2020/07/22/S945/CROSS/20200722-S945_CROSS_ALL_0_0",
                "size": 32822,
                "md5sum": "936c0515dcbc27d2e2fc3ebdcf5f883a",
                "createdAt": "2020-07-22T22:40:41",
                "updatedAt": "2020-07-22T22:40:41",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Thu, 30 Jul 2020 11:27:03 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )


@pytest.fixture
def mocked_files_available_to_download_single_source_single_day():
    set_of_files_available_to_download = [
        DiscoveredFileInfo(
            file_name="COREREF_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/"
                "CORE/20200722-S945_CORE_ALL_0_0"
            ),
            source_id=945,
            reference_date=datetime.datetime(year=2020, month=7, day=22),
            size=17734,
            md5sum="3548e03c8833b0e2133c80ac3b1dcdac",
        ),
        DiscoveredFileInfo(
            file_name="CROSSREF_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/"
                "CROSS/20200722-S945_CROSS_ALL_0_0"
            ),
            source_id=945,
            reference_date=datetime.datetime(year=2020, month=7, day=22),
            size=32822,
            md5sum="936c0515dcbc27d2e2fc3ebdcf5f883a",
        ),
        DiscoveredFileInfo(
            file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/"
                "WATCHLIST/20200722-S945_WATCHLIST_username_0_0"
            ),
            source_id=945,
            reference_date=datetime.datetime(year=2020, month=7, day=22),
            size=61663360,
            md5sum="78571e930fb12fcfb2fb70feb07c7bcf",
        ),
    ]
    return set_of_files_available_to_download


@pytest.fixture
def mocked_whole_files_download_details_single_source_single_day():
    list_of_download_details = [
        DownloadDetails(
            file_name="COREREF_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/CORE/"
                "20200722-S945_CORE_ALL_0_0"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
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
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/CROSS", "CROSSREF_945_20200722.txt.bz2"
            ),
            source_id=945,
            reference_date=datetime.datetime(year=2020, month=7, day=22),
            size=32822,
            md5sum="936c0515dcbc27d2e2fc3ebdcf5f883a",
            is_partitioned=False,
        ),
        DownloadDetails(
            file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722.txt.bz2"
            ),
            source_id=945,
            reference_date=datetime.datetime(year=2020, month=7, day=22),
            size=61663360,
            md5sum="78571e930fb12fcfb2fb70feb07c7bcf",
            is_partitioned=True,
        ),
    ]
    return list_of_download_details


@pytest.fixture
def mocked_whole_files_download_details_single_source_single_day_synchronous_case():
    list_of_download_details = [
        DownloadDetails(
            file_name="COREREF_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/CORE/"
                "20200722-S945_CORE_ALL_0_0"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/CORE", "COREREF_945_20200722.txt.bz2"
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
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/CROSS", "CROSSREF_945_20200722.txt.bz2"
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
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722.txt.bz2"
            ),
            source_id=945,
            reference_date=datetime.datetime(year=2020, month=7, day=22),
            size=61663360,
            md5sum="78571e930fb12fcfb2fb70feb07c7bcf",
            is_partitioned=None,
        ),
    ]
    return list_of_download_details


@pytest.fixture
def mocked_partitions_download_details_single_source_single_day():
    return [
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0?start=0&end=5242880"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722_1.txt"
            ),
            partition_index=1,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0?start=5242881&end=10485760"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722_2.txt"
            ),
            partition_index=2,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0?start=10485761&end=15728640"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722_3.txt"
            ),
            partition_index=3,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0?start=15728641&end=20971520"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722_4.txt"
            ),
            partition_index=4,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0?start=20971521&end=26214400"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722_5.txt"
            ),
            partition_index=5,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0?start=26214401&end=31457280"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722_6.txt"
            ),
            partition_index=6,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0?start=31457281&end=36700160"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722_7.txt"
            ),
            partition_index=7,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0?start=36700161&end=41943040"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722_8.txt"
            ),
            partition_index=8,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0?start=41943041&end=47185920"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722_9.txt"
            ),
            partition_index=9,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0?start=47185921&end=52428800"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722_10.txt"
            ),
            partition_index=10,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0?start=52428801&end=57671680"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722_11.txt"
            ),
            partition_index=11,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_945_20200722.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/22/S945/WATCHLIST/"
                "20200722-S945_WATCHLIST_username_0_0?start=57671681&end=61663360"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/22/S945/WATCHLIST", "WATCHLIST_945_20200722_12.txt"
            ),
            partition_index=12,
        ),
    ]


""""Datavault API with single source and multiple days."""


@pytest.fixture
def mocked_datavault_api_single_source_multiple_days(mocked_response):
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list",
        json=[
            {
                "name": "2020",
                "parent": "/v2/list",
                "url": "/v2/list/2020",
                "size": 0,
                "createdAt": "2020-01-01T00:00:00",
                "updatedAt": "2020-07-30T00:00:00",
                "writable": False,
                "directory": True,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Tue, 04 Aug 2020 09:14:00 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, "
            "content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers": "Cache-Control, Content-Language, "
            "Content-Length, Content-Type"
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020",
        json=[
            {
                "name": "07",
                "parent": "/v2/list/2020",
                "url": "/v2/list/2020/07",
                "size": 0,
                "createdAt": "2020-07-01T00:00:00",
                "updatedAt": "2020-07-30T00:00:00",
                "writable": False,
                "directory": True,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Tue, 04 Aug 2020 09:15:28 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, "
            "content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers": "Cache-Control, Content-Language,"
            " Content-Length, Content-Type"
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07",
        json=[
            {
                "name": "20",
                "parent": "/v2/list/2020/07",
                "url": "/v2/list/2020/07/20",
                "size": 0,
                "createdAt": "2020-07-20T22:08:28",
                "updatedAt": "2020-07-23T22:02:26",
                "writable": False,
                "directory": True,
            },
            {
                "name": "17",
                "parent": "/v2/list/2020/07",
                "url": "/v2/list/2020/07/17",
                "size": 0,
                "createdAt": "2020-07-17T23:45:36",
                "updatedAt": "2020-07-20T07:48:01",
                "writable": False,
                "directory": True,
            },
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Tue, 04 Aug 2020 09:16:33 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, "
            "content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers": "Cache-Control, Content-Language, "
            "Content-Length, Content-Type"
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/20",
        json=[
            {
                "name": "S207",
                "parent": "/v2/list/2020/07/20",
                "url": "/v2/list/2020/07/20/S207",
                "size": 0,
                "createdAt": "2020-07-21T06:35:36",
                "updatedAt": "2020-07-21T06:41:03",
                "writable": False,
                "directory": True,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Tue, 04 Aug 2020 09:19:10 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, "
            "content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers": "Cache-Control, Content-Language, "
            "Content-Length, Content-Type"
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/20/S207",
        json=[
            {
                "name": "CORE",
                "parent": "/v2/list/2020/07/20/S207",
                "url": "/v2/list/2020/07/20/S207/CORE",
                "size": 0,
                "createdAt": "2020-07-21T06:41:03",
                "updatedAt": "2020-07-21T06:41:03",
                "writable": False,
                "directory": True,
            },
            {
                "name": "CROSS",
                "parent": "/v2/list/2020/07/20/S207",
                "url": "/v2/list/2020/07/20/S207/CROSS",
                "size": 0,
                "createdAt": "2020-07-21T06:38:41",
                "updatedAt": "2020-07-21T06:38:41",
                "writable": False,
                "directory": True,
            },
            {
                "name": "WATCHLIST",
                "parent": "/v2/list/2020/07/20/S207",
                "url": "/v2/list/2020/07/20/S207/WATCHLIST",
                "size": 0,
                "createdAt": "2020-07-21T06:35:36",
                "updatedAt": "2020-07-21T06:35:36",
                "writable": False,
                "directory": True,
            },
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Tue, 04 Aug 2020 09:21:32 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization,"
            " content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers": "Cache-Control, Content-Language,"
            " Content-Length, Content-Type"
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/20/S207/CORE",
        json=[
            {
                "name": "COREREF_207_20200720.txt.bz2",
                "fid": "20200720-S207_CORE_ALL_0_0",
                "parent": "/v2/list/2020/07/20/S207/CORE",
                "url": "/v2/data/2020/07/20/S207/CORE/20200720-S207_CORE_ALL_0_0",
                "size": 4548016,
                "md5sum": "a46a5f07b6a402d4023ef550df6a12e4",
                "createdAt": "2020-07-21T06:41:03",
                "updatedAt": "2020-07-21T06:41:03",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Tue, 04 Aug 2020 09:24:37 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization,"
            " content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers": "Cache-Control, Content-Language,"
            " Content-Length, Content-Type"
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/20/S207/CROSS",
        json=[
            {
                "name": "CROSSREF_207_20200720.txt.bz2",
                "fid": "20200720-S207_CROSS_ALL_0_0",
                "parent": "/v2/list/2020/07/20/S207/CROSS",
                "url": "/v2/data/2020/07/20/S207/CROSS/20200720-S207_CROSS_ALL_0_0",
                "size": 14571417,
                "md5sum": "6b3dbd152e7dccf4147f62b6ce1c78c3",
                "createdAt": "2020-07-21T06:38:41",
                "updatedAt": "2020-07-21T06:38:41",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Tue, 04 Aug 2020 09:26:11 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/20/S207/WATCHLIST",
        json=[
            {
                "name": "WATCHLIST_username_207_20200720.txt.bz2",
                "fid": "20200720-S207_WATCHLIST_username_0_0",
                "parent": "/v2/list/2020/07/20/S207/WATCHLIST",
                "url": "/v2/data/2020/07/20/S207/WATCHLIST/20200720-S207_WATCHLIST_username_0_0",
                "size": 70613654,
                "md5sum": "ba2c00511520a3cf4b5383ceedb3b41d",
                "createdAt": "2020-07-21T06:35:36",
                "updatedAt": "2020-07-21T06:35:36",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Tue, 04 Aug 2020 09:27:51 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/17",
        json=[
            {
                "name": "S207",
                "parent": "/v2/list/2020/07/17",
                "url": "/v2/list/2020/07/17/S207",
                "size": 0,
                "createdAt": "2020-07-18T07:02:07",
                "updatedAt": "2020-07-18T07:07:02",
                "writable": False,
                "directory": True,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Tue, 04 Aug 2020 09:30:40 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/17/S207",
        json=[
            {
                "name": "CORE",
                "parent": "/v2/list/2020/07/17/S207",
                "url": "/v2/list/2020/07/17/S207/CORE",
                "size": 0,
                "createdAt": "2020-07-18T07:07:02",
                "updatedAt": "2020-07-18T07:07:02",
                "writable": False,
                "directory": True,
            },
            {
                "name": "CROSS",
                "parent": "/v2/list/2020/07/17/S207",
                "url": "/v2/list/2020/07/17/S207/CROSS",
                "size": 0,
                "createdAt": "2020-07-18T07:05:13",
                "updatedAt": "2020-07-18T07:05:13",
                "writable": False,
                "directory": True,
            },
            {
                "name": "WATCHLIST",
                "parent": "/v2/list/2020/07/17/S207",
                "url": "/v2/list/2020/07/17/S207/WATCHLIST",
                "size": 0,
                "createdAt": "2020-07-18T07:02:07",
                "updatedAt": "2020-07-18T07:02:07",
                "writable": False,
                "directory": True,
            },
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Tue, 04 Aug 2020 09:32:26 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/17/S207/CORE",
        json=[
            {
                "name": "COREREF_207_20200717.txt.bz2",
                "fid": "20200717-S207_CORE_ALL_0_0",
                "parent": "/v2/list/2020/07/17/S207/CORE",
                "url": "/v2/data/2020/07/17/S207/CORE/20200717-S207_CORE_ALL_0_0",
                "size": 3910430,
                "md5sum": "63958e5bc651b95da410e76a1763dde7",
                "createdAt": "2020-07-18T07:07:02",
                "updatedAt": "2020-07-18T07:07:02",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Tue, 04 Aug 2020 09:34:45 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/17/S207/CROSS",
        json=[
            {
                "name": "CROSSREF_207_20200717.txt.bz2",
                "fid": "20200717-S207_CROSS_ALL_0_0",
                "parent": "/v2/list/2020/07/17/S207/CROSS",
                "url": "/v2/data/2020/07/17/S207/CROSS/20200717-S207_CROSS_ALL_0_0",
                "size": 13816558,
                "md5sum": "d1316740714e9b13cf03acf02a23c596",
                "createdAt": "2020-07-18T07:05:13",
                "updatedAt": "2020-07-18T07:05:13",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Tue, 04 Aug 2020 09:36:58 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/17/S207/WATCHLIST",
        json=[
            {
                "name": "WATCHLIST_username_207_20200717.txt.bz2",
                "fid": "20200717-S207_WATCHLIST_username_0_0",
                "parent": "/v2/list/2020/07/17/S207/WATCHLIST",
                "url": "/v2/data/2020/07/17/S207/WATCHLIST/20200717-S207_WATCHLIST_username_0_0",
                "size": 63958346,
                "md5sum": "9be9099186dfd8a7e0012e58fd49a3da",
                "createdAt": "2020-07-18T07:02:07",
                "updatedAt": "2020-07-18T07:02:07",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Tue, 04 Aug 2020 09:38:30 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )


@pytest.fixture
def mocked_files_available_to_download_single_source_multiple_days():
    set_of_files_available_to_download = [
        DiscoveredFileInfo(
            file_name="COREREF_207_20200717.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/17/S207/CORE/"
                "20200717-S207_CORE_ALL_0_0"
            ),
            source_id=207,
            reference_date=datetime.datetime(year=2020, month=7, day=17),
            size=3910430,
            md5sum="63958e5bc651b95da410e76a1763dde7",
        ),
        DiscoveredFileInfo(
            file_name="CROSSREF_207_20200717.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/17/S207/CROSS/"
                "20200717-S207_CROSS_ALL_0_0"
            ),
            source_id=207,
            reference_date=datetime.datetime(year=2020, month=7, day=17),
            size=13816558,
            md5sum="d1316740714e9b13cf03acf02a23c596",
        ),
        DiscoveredFileInfo(
            file_name="WATCHLIST_207_20200717.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/17/S207/WATCHLIST/"
                "20200717-S207_WATCHLIST_username_0_0"
            ),
            source_id=207,
            reference_date=datetime.datetime(year=2020, month=7, day=17),
            size=63958346,
            md5sum="9be9099186dfd8a7e0012e58fd49a3da",
        ),
        DiscoveredFileInfo(
            file_name="COREREF_207_20200720.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/CORE/"
                "20200720-S207_CORE_ALL_0_0"
            ),
            source_id=207,
            reference_date=datetime.datetime(year=2020, month=7, day=20),
            size=4548016,
            md5sum="a46a5f07b6a402d4023ef550df6a12e4",
        ),
        DiscoveredFileInfo(
            file_name="CROSSREF_207_20200720.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/CROSS/"
                "20200720-S207_CROSS_ALL_0_0"
            ),
            source_id=207,
            reference_date=datetime.datetime(year=2020, month=7, day=20),
            size=14571417,
            md5sum="6b3dbd152e7dccf4147f62b6ce1c78c3",
        ),
        DiscoveredFileInfo(
            file_name="WATCHLIST_207_20200720.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/WATCHLIST/"
                "20200720-S207_WATCHLIST_username_0_0"
            ),
            source_id=207,
            reference_date=datetime.datetime(year=2020, month=7, day=20),
            size=70613654,
            md5sum="ba2c00511520a3cf4b5383ceedb3b41d",
        ),
    ]
    return set_of_files_available_to_download


@pytest.fixture
def mocked_download_info_single_source_multiple_days_synchronous():
    set_of_files_available_to_download = [
        DownloadDetails(
            file_name='COREREF_207_20200717.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/17/S207/CORE/'
                '20200717-S207_CORE_ALL_0_0'
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                'Temp/Data/', '2020/07/17/S207/CORE/COREREF_207_20200717.txt.bz2'
            ),
            source_id=207,
            reference_date=datetime.datetime(2020, 7, 17, 0, 0),
            size=3910430,
            md5sum='63958e5bc651b95da410e76a1763dde7',
            is_partitioned=None,
        ),
        DownloadDetails(
            file_name='CROSSREF_207_20200717.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/17/S207/'
                'CROSS/20200717-S207_CROSS_ALL_0_0'
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                'Temp/Data/', '2020/07/17/S207/CROSS/CROSSREF_207_20200717.txt.bz2'
            ),
            source_id=207, reference_date=datetime.datetime(2020, 7, 17, 0, 0),
            size=13816558, md5sum='d1316740714e9b13cf03acf02a23c596',
            is_partitioned=None,
        ),
        DownloadDetails(
            file_name='WATCHLIST_207_20200717.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/17/S207/WATCHLIST/'
                '20200717-S207_WATCHLIST_username_0_0'
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                'Temp/Data/', '2020/07/17/S207/WATCHLIST/WATCHLIST_207_20200717.txt.bz2'
            ),
            source_id=207, reference_date=datetime.datetime(2020, 7, 17, 0, 0),
            size=63958346, md5sum='9be9099186dfd8a7e0012e58fd49a3da',
            is_partitioned=None,
        ),
        DownloadDetails(
            file_name='COREREF_207_20200720.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/CORE/'
                '20200720-S207_CORE_ALL_0_0'
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                'Temp/Data/', '2020/07/20/S207/CORE/COREREF_207_20200720.txt.bz2'
            ),
            source_id=207, reference_date=datetime.datetime(2020, 7, 20, 0, 0),
            size=4548016, md5sum='a46a5f07b6a402d4023ef550df6a12e4',
            is_partitioned=None,
        ),
        DownloadDetails(
            file_name='CROSSREF_207_20200720.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/CROSS/'
                '20200720-S207_CROSS_ALL_0_0'
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                'Temp/Data/', '2020/07/20/S207/CROSS/CROSSREF_207_20200720.txt.bz2'
            ),
            source_id=207, reference_date=datetime.datetime(2020, 7, 20, 0, 0),
            size=14571417, md5sum='6b3dbd152e7dccf4147f62b6ce1c78c3',
            is_partitioned=None,
        ),
        DownloadDetails(
            file_name='WATCHLIST_207_20200720.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/WATCHLIST/'
                '20200720-S207_WATCHLIST_username_0_0'
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                'Temp/Data/', '2020/07/20/S207/WATCHLIST/WATCHLIST_207_20200720.txt.bz2'
            ),
            source_id=207, reference_date=datetime.datetime(2020, 7, 20, 0, 0),
            size=70613654, md5sum='ba2c00511520a3cf4b5383ceedb3b41d',
            is_partitioned=None,
        ),
    ]
    return set_of_files_available_to_download


@pytest.fixture
def mocked_download_info_single_source_multiple_days_concurrent():
    set_of_files_available_to_download = [
        DownloadDetails(
            file_name='COREREF_207_20200717.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/17/S207/CORE/'
                '20200717-S207_CORE_ALL_0_0'
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                'Temp/Data/2020/07/17/S207/CORE/COREREF_207_20200717.txt.bz2',
            ),
            source_id=207,
            reference_date=datetime.datetime(2020, 7, 17, 0, 0),
            size=3910430,
            md5sum='63958e5bc651b95da410e76a1763dde7',
            is_partitioned=False,
        ),
        DownloadDetails(
            file_name='CROSSREF_207_20200717.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/17/S207/CROSS/'
                '20200717-S207_CROSS_ALL_0_0'
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                'Temp/Data/2020/07/17/S207/CROSS/CROSSREF_207_20200717.txt.bz2',
            ),
            source_id=207,
            reference_date=datetime.datetime(2020, 7, 17, 0, 0),
            size=13816558,
            md5sum='d1316740714e9b13cf03acf02a23c596',
            is_partitioned=False,
        ),
        DownloadDetails(
            file_name='WATCHLIST_207_20200717.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/17/S207/WATCHLIST/'
                '20200717-S207_WATCHLIST_username_0_0'
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                'Temp/Data/2020/07/17/S207/WATCHLIST/WATCHLIST_207_20200717.txt.bz2',
            ),
            source_id=207,
            reference_date=datetime.datetime(2020, 7, 17, 0, 0),
            size=63958346,
            md5sum='9be9099186dfd8a7e0012e58fd49a3da',
            is_partitioned=True,
        ),
        DownloadDetails(
            file_name='COREREF_207_20200720.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/CORE/'
                '20200720-S207_CORE_ALL_0_0'
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                'Temp/Data/2020/07/20/S207/CORE/COREREF_207_20200720.txt.bz2',
            ),
            source_id=207,
            reference_date=datetime.datetime(2020, 7, 20, 0, 0),
            size=4548016,
            md5sum='a46a5f07b6a402d4023ef550df6a12e4',
            is_partitioned=False,
        ),
        DownloadDetails(
            file_name='CROSSREF_207_20200720.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/CROSS/'
                '20200720-S207_CROSS_ALL_0_0'
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                'Temp/Data/2020/07/20/S207/CROSS/CROSSREF_207_20200720.txt.bz2',
            ),
            source_id=207,
            reference_date=datetime.datetime(2020, 7, 20, 0, 0),
            size=14571417,
            md5sum='6b3dbd152e7dccf4147f62b6ce1c78c3',
            is_partitioned=False,
        ),
        DownloadDetails(
            file_name='WATCHLIST_207_20200720.txt.bz2',
            download_url=(
                'https://api.icedatavault.icedataservices.com/v2/data/2020/07/20/S207/WATCHLIST/'
                '20200720-S207_WATCHLIST_username_0_0'
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                'Temp/Data/2020/07/20/S207/WATCHLIST/WATCHLIST_207_20200720.txt.bz2'
            ),
            source_id=207,
            reference_date=datetime.datetime(2020, 7, 20, 0, 0),
            size=70613654,
            md5sum='ba2c00511520a3cf4b5383ceedb3b41d',
            is_partitioned=True,
        )
    ]
    return set_of_files_available_to_download


"""Datavault API with multiple sources over a single day."""


@pytest.fixture
def mocked_datavault_api_multiple_sources_single_day(mocked_response):
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list",
        json=[
            {
                "name": "2020",
                "parent": "/v2/list",
                "url": "/v2/list/2020",
                "size": 0,
                "createdAt": "2020-01-01T00:00:00",
                "updatedAt": "2020-08-05T00:00:00",
                "writable": False,
                "directory": True,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Wed, 05 Aug 2020 10:23:14 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020",
        json=[
            {
                "name": "07",
                "parent": "/v2/list/2020",
                "url": "/v2/list/2020/07",
                "size": 0,
                "createdAt": "2020-07-01T00:00:00",
                "updatedAt": "2020-07-31T00:00:00",
                "writable": False,
                "directory": True,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Wed, 05 Aug 2020 10:33:34 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07",
        json=[
            {
                "name": "21",
                "parent": "/v2/list/2020/07",
                "url": "/v2/list/2020/07/21",
                "size": 0,
                "createdAt": "2020-07-21T22:00:49",
                "updatedAt": "2020-07-23T21:34:01",
                "writable": False,
                "directory": True,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Wed, 05 Aug 2020 10:35:25 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/21",
        json=[
            {
                "name": "S367",
                "parent": "/v2/list/2020/07/21",
                "url": "/v2/list/2020/07/21/S367",
                "size": 0,
                "createdAt": "2020-07-22T00:59:44",
                "updatedAt": "2020-07-23T15:28:41",
                "writable": False,
                "directory": True,
            },
            {
                "name": "S207",
                "parent": "/v2/list/2020/07/21",
                "url": "/v2/list/2020/07/21/S207",
                "size": 0,
                "createdAt": "2020-07-22T06:36:31",
                "updatedAt": "2020-07-22T06:43:36",
                "writable": False,
                "directory": True,
            },
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Wed, 05 Aug 2020 10:38:21 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/21/S367",
        json=[
            {
                "name": "CORE",
                "parent": "/v2/list/2020/07/21/S367",
                "url": "/v2/list/2020/07/21/S367/CORE",
                "size": 0,
                "createdAt": "2020-07-22T01:00:24",
                "updatedAt": "2020-07-23T15:23:11",
                "writable": False,
                "directory": True,
            },
            {
                "name": "CROSS",
                "parent": "/v2/list/2020/07/21/S367",
                "url": "/v2/list/2020/07/21/S367/CROSS",
                "size": 0,
                "createdAt": "2020-07-22T00:59:44",
                "updatedAt": "2020-07-23T15:28:41",
                "writable": False,
                "directory": True,
            },
            {
                "name": "WATCHLIST",
                "parent": "/v2/list/2020/07/21/S367",
                "url": "/v2/list/2020/07/21/S367/WATCHLIST",
                "size": 0,
                "createdAt": "2020-07-22T01:00:06",
                "updatedAt": "2020-07-22T01:00:06",
                "writable": False,
                "directory": True,
            },
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Wed, 05 Aug 2020 10:43:26 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/21/S367/CORE",
        json=[
            {
                "name": "COREREF_367_20200721.txt.bz2",
                "fid": "20200721-S367_CORE_ALL_0_0",
                "parent": "/v2/list/2020/07/21/S367/CORE",
                "url": "/v2/data/2020/07/21/S367/CORE/20200721-S367_CORE_ALL_0_0",
                "size": 706586,
                "md5sum": "e28385e918aa71720235232c9a895b64",
                "createdAt": "2020-07-22T01:00:24",
                "updatedAt": "2020-07-23T15:23:11",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Wed, 05 Aug 2020 10:46:15 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/21/S367/CROSS",
        json=[
            {
                "name": "CROSSREF_367_20200721.txt.bz2",
                "fid": "20200721-S367_CROSS_ALL_0_0",
                "parent": "/v2/list/2020/07/21/S367/CROSS",
                "url": "/v2/data/2020/07/21/S367/CROSS/20200721-S367_CROSS_ALL_0_0",
                "size": 879897,
                "md5sum": "fdb7592c8806a28f59c4d4da1e934c43",
                "createdAt": "2020-07-22T00:59:44",
                "updatedAt": "2020-07-23T15:28:41",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Wed, 05 Aug 2020 10:46:30 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/21/S367/WATCHLIST",
        json=[
            {
                "name": "WATCHLIST_username_367_20200721.txt.bz2",
                "fid": "20200721-S367_WATCHLIST_username_0_0",
                "parent": "/v2/list/2020/07/21/S367/WATCHLIST",
                "url": "/v2/data/2020/07/21/S367/WATCHLIST/20200721-S367_WATCHLIST_username_0_0",
                "size": 82451354,
                "md5sum": "62df718ef5eb5f9f1ea3f6ea1f826c30",
                "createdAt": "2020-07-22T01:00:06",
                "updatedAt": "2020-07-22T01:00:06",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Wed, 05 Aug 2020 10:46:44 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/21/S207",
        json=[
            {
                "name": "CORE",
                "parent": "/v2/list/2020/07/21/S207",
                "url": "/v2/list/2020/07/21/S207/CORE",
                "size": 0,
                "createdAt": "2020-07-22T06:43:36",
                "updatedAt": "2020-07-22T06:43:36",
                "writable": False,
                "directory": True,
            },
            {
                "name": "CROSS",
                "parent": "/v2/list/2020/07/21/S207",
                "url": "/v2/list/2020/07/21/S207/CROSS",
                "size": 0,
                "createdAt": "2020-07-22T06:41:50",
                "updatedAt": "2020-07-22T06:41:50",
                "writable": False,
                "directory": True,
            },
            {
                "name": "WATCHLIST",
                "parent": "/v2/list/2020/07/21/S207",
                "url": "/v2/list/2020/07/21/S207/WATCHLIST",
                "size": 0,
                "createdAt": "2020-07-22T06:36:31",
                "updatedAt": "2020-07-22T06:36:31",
                "writable": False,
                "directory": True,
            },
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Wed, 05 Aug 2020 10:52:19 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/21/S207/CORE",
        json=[
            {
                "name": "COREREF_207_20200721.txt.bz2",
                "fid": "20200721-S207_CORE_ALL_0_0",
                "parent": "/v2/list/2020/07/21/S207/CORE",
                "url": "/v2/data/2020/07/21/S207/CORE/20200721-S207_CORE_ALL_0_0",
                "size": 4590454,
                "md5sum": "c1a079841f84676e91b5021afd3f5272",
                "createdAt": "2020-07-22T06:43:36",
                "updatedAt": "2020-07-22T06:43:36",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Wed, 05 Aug 2020 11:00:59 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/21/S207/CROSS",
        json=[
            {
                "name": "CROSSREF_207_20200721.txt.bz2",
                "fid": "20200721-S207_CROSS_ALL_0_0",
                "parent": "/v2/list/2020/07/21/S207/CROSS",
                "url": "/v2/data/2020/07/21/S207/CROSS/20200721-S207_CROSS_ALL_0_0",
                "size": 14690557,
                "md5sum": "f2683cd87a7b29f3b8776373d56a8456",
                "createdAt": "2020-07-22T06:41:50",
                "updatedAt": "2020-07-22T06:41:50",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Wed, 05 Aug 2020 11:01:25 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )
    mocked_response.add(
        responses.GET,
        url="https://api.icedatavault.icedataservices.com/v2/list/2020/07/21/S207/WATCHLIST",
        json=[
            {
                "name": "WATCHLIST_username_207_20200721.txt.bz2",
                "fid": "20200721-S207_WATCHLIST_username_0_0",
                "parent": "/v2/list/2020/07/21/S207/WATCHLIST",
                "url": "/v2/data/2020/07/21/S207/WATCHLIST/20200721-S207_WATCHLIST_username_0_0",
                "size": 72293374,
                "md5sum": "36e444a8362e7db52af50ee0f8dc0d2e",
                "createdAt": "2020-07-22T06:36:31",
                "updatedAt": "2020-07-22T06:36:31",
                "writable": False,
                "directory": False,
            }
        ],
        status=200,
        content_type="application/json;charset=UTF-8",
        headers={
            "Date": "Wed, 05 Aug 2020 11:02:08 GMT",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE, PUT",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Headers": "x-request-with, authorization, content-type",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Expose-Headers":
                "Cache-Control, Content-Language, Content-Length, Content-Type, "
            "Expires, Last-Modified, Pragma",
            "X-Content-Type-Options": "nosniff",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Strict-Transport-Security": "max-age=31536000 ; includeSubDomains",
            "X-Frame-Options": "DENY",
        },
    )


@pytest.fixture
def mocked_files_available_to_download_multiple_sources_single_day():
    set_of_files_available_to_download = [
        DiscoveredFileInfo(
            file_name="COREREF_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/CORE/"
                "20200721-S207_CORE_ALL_0_0"
            ),
            source_id=207,
            reference_date=datetime.datetime(year=2020, month=7, day=21),
            size=4590454,
            md5sum="c1a079841f84676e91b5021afd3f5272",
        ),
        DiscoveredFileInfo(
            file_name="COREREF_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/CORE/"
                "20200721-S367_CORE_ALL_0_0"
            ),
            source_id=367,
            reference_date=datetime.datetime(year=2020, month=7, day=21),
            size=706586,
            md5sum="e28385e918aa71720235232c9a895b64",
        ),
        DiscoveredFileInfo(
            file_name="CROSSREF_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/CROSS/"
                "20200721-S207_CROSS_ALL_0_0"
            ),
            source_id=207,
            reference_date=datetime.datetime(year=2020, month=7, day=21),
            size=14690557,
            md5sum="f2683cd87a7b29f3b8776373d56a8456",
        ),
        DiscoveredFileInfo(
            file_name="CROSSREF_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/CROSS/"
                "20200721-S367_CROSS_ALL_0_0"
            ),
            source_id=367,
            reference_date=datetime.datetime(year=2020, month=7, day=21),
            size=879897,
            md5sum="fdb7592c8806a28f59c4d4da1e934c43",
        ),
        DiscoveredFileInfo(
            file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0"
            ),
            source_id=207,
            reference_date=datetime.datetime(year=2020, month=7, day=21),
            size=72293374,
            md5sum="36e444a8362e7db52af50ee0f8dc0d2e",
        ),
        DiscoveredFileInfo(
            file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0"
            ),
            source_id=367,
            reference_date=datetime.datetime(year=2020, month=7, day=21),
            size=82451354,
            md5sum="62df718ef5eb5f9f1ea3f6ea1f826c30",
        ),
    ]
    return set_of_files_available_to_download


@pytest.fixture
def mocked_download_details_multiple_sources_single_day():
    download_details = [
        DownloadDetails(
            file_name="COREREF_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/"
                "S207/CORE/20200721-S207_CORE_ALL_0_0"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
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
            file_path=Path(__file__).resolve().parent.joinpath(
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
            file_path=Path(__file__).resolve().parent.joinpath(
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
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/CROSS", "CROSSREF_367_20200721.txt.bz2"
            ),
            source_id=367,
            reference_date=datetime.datetime(year=2020, month=7, day=21),
            size=879897,
            md5sum="fdb7592c8806a28f59c4d4da1e934c43",
            is_partitioned=False,
        ),
        DownloadDetails(
            file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
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
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721.txt.bz2"
            ),
            source_id=367,
            reference_date=datetime.datetime(year=2020, month=7, day=21),
            size=82451354,
            md5sum="62df718ef5eb5f9f1ea3f6ea1f826c30",
            is_partitioned=True,
        ),
    ]
    return download_details


@pytest.fixture
def mocked_partitions_download_details_multiple_sources_single_day():
    return [
        PartitionDownloadDetails(
            parent_file_name="CROSSREF_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/CROSS/"
                "20200721-S207_CROSS_ALL_0_0?start=0&end=5242880"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/CROSS", "CROSSREF_207_20200721_1.txt"
            ),
            partition_index=1,
        ),
        PartitionDownloadDetails(
            parent_file_name="CROSSREF_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/CROSS/"
                "20200721-S207_CROSS_ALL_0_0?start=5242881&end=10485760"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/CROSS", "CROSSREF_207_20200721_2.txt"
            ),
            partition_index=2,
        ),
        PartitionDownloadDetails(
            parent_file_name="CROSSREF_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/CROSS/"
                "20200721-S207_CROSS_ALL_0_0?start=10485761&end=14690557"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/CROSS", "CROSSREF_207_20200721_3.txt"
            ),
            partition_index=3,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=0&end=5242880"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_1.txt"
            ),
            partition_index=1,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=5242881&end=10485760"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_2.txt"
            ),
            partition_index=2,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=10485761&end=15728640"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_3.txt"
            ),
            partition_index=3,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=15728641&end=20971520"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_4.txt"
            ),
            partition_index=4,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=20971521&end=26214400"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_5.txt"
            ),
            partition_index=5,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=26214401&end=31457280"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_6.txt"
            ),
            partition_index=6,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=31457281&end=36700160"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_7.txt"
            ),
            partition_index=7,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=36700161&end=41943040"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_8.txt"
            ),
            partition_index=8,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=41943041&end=47185920"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_9.txt"
            ),
            partition_index=9,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=47185921&end=52428800"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_10.txt"
            ),
            partition_index=10,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=52428801&end=57671680"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_11.txt"
            ),
            partition_index=11,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=57671681&end=62914560"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_12.txt"
            ),
            partition_index=12,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=62914561&end=68157440"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_13.txt"
            ),
            partition_index=13,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_207_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S207/WATCHLIST/"
                "20200721-S207_WATCHLIST_username_0_0?start=68157441&end=72293374"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S207/WATCHLIST", "WATCHLIST_207_20200721_14.txt"
            ),
            partition_index=14,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=0&end=5242880"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_1.txt"
            ),
            partition_index=1,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=5242881&end=10485760"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_2.txt"
            ),
            partition_index=2,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=10485761&end=15728640"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_3.txt"
            ),
            partition_index=3,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=15728641&end=20971520"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_4.txt"
            ),
            partition_index=4,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=20971521&end=26214400"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_5.txt"
            ),
            partition_index=5,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=26214401&end=31457280"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_6.txt"
            ),
            partition_index=6,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=31457281&end=36700160"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_7.txt"
            ),
            partition_index=7,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=36700161&end=41943040"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_8.txt"
            ),
            partition_index=8,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=41943041&end=47185920"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_9.txt"
            ),
            partition_index=9,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=47185921&end=52428800"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_10.txt"
            ),
            partition_index=10,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=52428801&end=57671680"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_11.txt"
            ),
            partition_index=11,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=57671681&end=62914560"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_12.txt"
            ),
            partition_index=12,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=62914561&end=68157440"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_13.txt"
            ),
            partition_index=13,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=68157441&end=73400320"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_14.txt"
            ),
            partition_index=14,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=73400321&end=78643200"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_15.txt"
            ),
            partition_index=15,
        ),
        PartitionDownloadDetails(
            parent_file_name="WATCHLIST_367_20200721.txt.bz2",
            download_url=(
                "https://api.icedatavault.icedataservices.com/v2/data/2020/07/21/S367/WATCHLIST/"
                "20200721-S367_WATCHLIST_username_0_0?start=78643201&end=82451354"
            ),
            file_path=Path(__file__).resolve().parent.joinpath(
                "Data/2020/07/21/S367/WATCHLIST", "WATCHLIST_367_20200721_16.txt"
            ),
            partition_index=16,
        ),
    ]


"""Others."""


@pytest.fixture(scope="session")
def simulated_downloaded_partitions(tmp_path_factory):
    path_to_tmp_dir = tmp_path_factory.mktemp("Data")
    partition_file_names = [
        "WATCHLIST_367_20200721_1.txt",
        "WATCHLIST_367_20200721_2.txt",
        "WATCHLIST_367_20200721_3.txt",
        "WATCHLIST_367_20200721_4.txt",
        "WATCHLIST_367_20200721_5.txt",
        "WATCHLIST_367_20200721_6.txt",
        "WATCHLIST_367_20200721_7.txt",
        "WATCHLIST_367_20200721_8.txt",
        "WATCHLIST_367_20200721_9.txt",
        "WATCHLIST_367_20200721_10.txt",
        "WATCHLIST_367_20200721_11.txt",
        "WATCHLIST_367_20200721_12.txt",
        "WATCHLIST_367_20200721_13.txt",
        "WATCHLIST_367_20200721_14.txt",
        "WATCHLIST_367_20200721_15.txt",
    ]
    for name in partition_file_names:
        f_path = path_to_tmp_dir / name
        f_path.touch()
    return path_to_tmp_dir


@pytest.fixture()
def mocked_concurrent_download_manifest():
    download_manifest = ConcurrentDownloadManifest(
        files_reference_data=[
            DownloadDetails(
                file_name="COREREF_945_20201218.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/"
                    "20201218-S945_CORE_ALL_0_0"
                ),
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_name="CROSSREF_945_20201218.txt.bz2",
                download_url=(
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CROSS/"
                    "20201218-S945_CROSS_ALL_0_0"
                ),
                file_path=Path(__file__).resolve().parent.joinpath(
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
                    "https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/WATCHLIST"
                    "/20201218-S945_WATCHLIST_username_0_0"
                ),
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_name='CROSSREF_945_20201218.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CROSS/'
                    '20201218-S945_CROSS_ALL_0_0'
                ),
                file_path=Path(__file__).resolve().parent.joinpath(
                    "static_data/post_processing_scenario_1/2020/12/18/CROSS/"
                    "CROSSREF_945_20201218.txt.bz2"
                ),
                source_id=945,
                reference_date=datetime.datetime(2020, 12, 18, 0, 0),
                size=35150,
                md5sum='13da7cea9a7337cd71fd9aea4f909bc6',
                is_partitioned=False
            ),
        ],
        partitions_to_download=[
            PartitionDownloadDetails(
                parent_file_name='COREREF_945_20201218.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/CORE/'
                    '20201218-S945_CORE_ALL_0_0?start=0&end=5242880'
                ),
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_path=Path(__file__).resolve().parent.joinpath(
                    "static_data/post_processing_scenario_1/2020/12/18/CORE/"
                    "COREREF_945_20201218_5.txt"
                ),
                partition_index=5,
            ),
            PartitionDownloadDetails(
                parent_file_name='WATCHLIST_945_20201218.txt.bz2',
                download_url=(
                    'https://api.icedatavault.icedataservices.com/v2/data/2020/12/18/S945/'
                    'WATCHLIST/20201218-S945_WATCHLIST_username_0_0?start=0&end=5242880'
                ),
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_path=Path(__file__).resolve().parent.joinpath(
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
                file_path=Path(__file__).resolve().parent.joinpath(
                    "static_data/post_processing_scenario_1/2020/12/18/WATCHLIST/"
                    "WATCHLIST_945_20201218_10.txt"
                ),
                partition_index=10,
            ),
        ]
    )
    return download_manifest
