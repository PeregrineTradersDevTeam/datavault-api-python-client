# This file is licensed under the terms of the MIT License.
# See the LICENSE file in the root of this repository
# for complete details.

"""Datavault API Client Library for Python."""


from datavault_api_client import (
    connectivity,
    crawler,
    data_integrity,
    data_structures,
    downloaders,
    helpers,
    post_download_processing,
    pre_download_processing,
)


__version__ = "1.0.0"

__title__ = "datavault_api_client"
__description__ = "Datavault API Client Library for Python"

__author__ = "Jacopo Abbate"
__email__ = "jacopo.abbate@peregrinetraders.com"

__license__ = "MIT License"
__copyright__ = "copyright (c) 2020 Peregrine Traders B.V."


__all__ = [
    "connectivity",
    "crawler",
    "data_integrity",
    "data_structures",
    "downloaders",
    "helpers",
    "post_download_processing",
    "pre_download_processing",
]
