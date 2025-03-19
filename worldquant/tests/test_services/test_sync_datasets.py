import unittest
from unittest.mock import patch, MagicMock
from worldquant.services.sync_datasets import sync_datasets


class TestSyncDatasets(unittest.TestCase):
    @patch("worldquant.services.sync_datasets.get_credentials")
    @patch("worldquant.services.sync_datasets.create_client")
    @patch("worldquant.services.sync_datasets.with_session")
    def test_sync_datasets(
        self, mock_with_session, mock_create_client, mock_get_credentials
    ):
        mock_client = MagicMock()
        mock_session = MagicMock()
        mock_with_session.return_value = lambda func: func(mock_session)
        mock_create_client.return_value = mock_client
        mock_get_credentials.return_value = {"username": "test", "password": "test"}

        sync_datasets(dataset_id="123", region="US", universe="TOP1000", delay=1)

        mock_client.get_datasets.assert_called()
        mock_session.commit.assert_called()


if __name__ == "__main__":
    unittest.main()
