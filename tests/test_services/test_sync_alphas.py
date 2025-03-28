import unittest
from unittest.mock import patch, MagicMock
from alphapower.services.sync_alphas import sync_alphas


class TestSyncAlphas(unittest.TestCase):
    @patch("worldquant.services.sync_alphas.get_credentials")
    @patch("worldquant.services.sync_alphas.create_client")
    @patch("worldquant.services.sync_alphas.with_session")
    def test_sync_alphas(
        self, mock_with_session, mock_create_client, mock_get_credentials
    ):
        mock_client = MagicMock()
        mock_session = MagicMock()
        mock_with_session.return_value = lambda func: func(mock_session)
        mock_create_client.return_value = mock_client
        mock_get_credentials.return_value = {"username": "test", "password": "test"}

        sync_alphas(start_time=None, end_time=None)

        mock_client.get_self_alphas.assert_called()
        mock_session.commit.assert_called()


if __name__ == "__main__":
    unittest.main()
