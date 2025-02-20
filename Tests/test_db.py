import sys
sys.path.append("..")
import unittest
import shutil
from lstore.db import Database
from BTrees.OOBTree import OOBTree

class TestDatabaseOpenClose(unittest.TestCase):

    def setUp(self):
        """Create a test database directory before each test"""
        self.test_db_path = "test_db"
        self.db = Database()
        self.db.open(self.test_db_path)

    def test_open_close_persistence(self):
        """Test that tables persist across database open/close cycles"""

        # create 3 tables
        table1 = self.db.create_table("Users", 5, 0)
        table2 = self.db.create_table("Orders", 4, 1)
        table3 = self.db.create_table("Products", 3, 0)

        # modify metadata within each table
        table1.page_directory = {0: "PageData1"}
        table2.page_directory = {1: "PageData2"}
        table3.page_directory = {2: "PageData3"}

        table1.index.create_index(2)  # Index on column 2
        table2.index.create_index(3)  # Index on column 3
        table1.index.insert_to_index(2, 50, 1)
        table2.index.insert_to_index(3, 99, 2)

        # close the database, which should write metadata to disk
        self.db.close()

        # re-open database - new instance
        new_db = Database()
        new_db.open(self.test_db_path)

        # ensure that all tables exist
        self.assertIn("Users", new_db.tables)
        self.assertIn("Orders", new_db.tables)
        self.assertIn("Products", new_db.tables)

        # verify that metadata was correctly written to disk via retreival
        users_table = new_db.tables["Users"]
        orders_table = new_db.tables["Orders"]
        products_table = new_db.tables["Products"]

        self.assertEqual(users_table.num_columns, 5)
        self.assertEqual(users_table.key, 0)
        self.assertEqual(users_table.page_directory, {0: "PageData1"})

        self.assertEqual(orders_table.num_columns, 4)
        self.assertEqual(orders_table.key, 1)
        self.assertEqual(orders_table.page_directory, {1: "PageData2"})

        self.assertEqual(products_table.num_columns, 3)
        self.assertEqual(products_table.key, 0)
        self.assertEqual(products_table.page_directory, {2: "PageData3"})

        self.assertIsInstance(users_table.index.indices[2], OOBTree)
        self.assertIsInstance(orders_table.index.indices[3], OOBTree)
        self.assertEqual(users_table.index.indices[2][50], {1: True})
        self.assertEqual(orders_table.index.indices[3][99], {2: True}) 

    def tearDown(self):
        """Remove the test database directory after tests"""
        shutil.rmtree(self.test_db_path, ignore_errors=True)

if __name__ == "__main__":
    unittest.main()
