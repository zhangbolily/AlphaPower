import unittest
from unittest.mock import patch, MagicMock
from worldquant.services.sync_datafields import sync_datafields


class TestSyncDatafields(unittest.TestCase):
    @patch("worldquant.services.sync_datafields.get_credentials")
    @patch("worldquant.services.sync_datafields.create_client")
    @patch("worldquant.services.sync_datafields.with_session")
    def test_sync_datafields(
        self, mock_with_session, mock_create_client, mock_get_credentials
    ):
        mock_client = MagicMock()
        mock_session = MagicMock()
        mock_with_session.return_value = lambda func: func(mock_session)
        mock_create_client.return_value = mock_client
        mock_get_credentials.return_value = {"username": "test", "password": "test"}

        sync_datafields(instrument_type="EQUITY", max_workers=2)

        mock_client.get_data_fields_in_dataset.assert_called()
        mock_session.commit.assert_called()


if __name__ == "__main__":
    unittest.main()
