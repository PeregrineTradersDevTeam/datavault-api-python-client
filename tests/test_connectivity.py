import pytest

import datavault_api_client.connectivity


class TestGetSession:
    @pytest.mark.parametrize(
        'total_retries', [
            2, 5, 10, 12
        ]
    )
    def test_total_retries(self, total_retries):
        # Setup
        session = datavault_api_client.connectivity.create_session(total_retries)
        # Exercise
        total_retries_ex_created_session = session.adapters.get('https://').max_retries.total
        # Verify
        assert total_retries_ex_created_session == total_retries
        # Cleanup - none

    @pytest.mark.parametrize(
        'backoff_factor', [
            0.1, 0.5, 1, 2
        ]
    )
    def test_backoff_factor(self, backoff_factor):
        # Setup
        session = datavault_api_client.connectivity.create_session(backoff_factor=backoff_factor)
        # Exercise
        backoff_factor = session.adapters.get('https://').max_retries.backoff_factor
        # Verify
        assert backoff_factor == backoff_factor
        # Cleanup - none

    @pytest.mark.parametrize(
        'status_forcelist, expected_status_forcelist', [
            ((), (401, 500, 502, 503, 504)),
            ((401,), (401, )),
            ((401, 500), (401, 500)),
            ((500, 502, 504), (500, 502, 504))
        ]
    )
    def test_status_forcelist(self, status_forcelist, expected_status_forcelist):
        # Setup
        if len(status_forcelist) == 0:
            session = datavault_api_client.connectivity.create_session()
        else:
            session = datavault_api_client.connectivity.create_session(
                status_forcelist=status_forcelist,
            )
        # Exercise
        status_forcelist =session.adapters.get('https://').max_retries.status_forcelist
        # Verify
        assert status_forcelist == expected_status_forcelist
        # Cleanup - none
