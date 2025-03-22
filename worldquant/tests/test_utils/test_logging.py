import unittest
import os
from worldquant.internal.utils.logging import setup_logging


class TestLogging(unittest.TestCase):
    def test_logging_setup(self):
        logger = setup_logging("test_module")
        logger.debug("This is a debug message")
        log_file = os.path.join("./logs", "test_module.log")
        self.assertTrue(os.path.exists(log_file))


if __name__ == "__main__":
    unittest.main()
