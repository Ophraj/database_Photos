import unittest
import Solution


class AbstractTest(unittest.TestCase):
    # before each test, setUp is executed
    def setUp(self) -> None:
        Solution.createTables()

    # after each test, tearDown is executed
    def tearDown(self) -> None:
        Solution.dropTables()
