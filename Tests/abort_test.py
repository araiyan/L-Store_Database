import sys
sys.path.append("..")

import unittest
from lstore.db import Database
from lstore.table import Table
from lstore.transaction import Transaction
from lstore.query import Query
import shutil
import os

class TestTransactionAbort(unittest.TestCase):
    def setUp(self):
        """Database setup"""
        self.db_path = "test_db"
        self.db = Database(self.db_path)
        self.db.open(self.db_path)
        self.table = self.db.create_table("Students", 5, 0)
        self.query = Query(self.table)

    def tearDown(self):
        """Cleanup DB after each test"""
        self.db.close()
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)

    def test_abort_insert(self):
        """Test insert rollback"""
        transaction = Transaction()
        transaction.add_query(self.query.insert, self.table, 1, 90, 80, 70, 60)
        transaction.abort()

        # check that record was not inserted
        self.assertFalse(self.query.select(1, 0, [1, 1, 1, 1, 1]))

    def test_abort_delete(self):
        """Test delete rollback"""
        # insert og record
        self.query.insert(2, 85, 75, 65, 55)

        # begin transaction and delete record, then abort
        transaction = Transaction()
        transaction.add_query(self.query.delete, self.table, 2)
        transaction.abort()

        # ensure record still exists
        result = self.query.select(2, 0, [1, 1, 1, 1, 1])
        self.assertTrue(result)

    def test_abort_update(self):
        """Test update rollback"""
        # insert og record
        self.query.insert(3, 88, 78, 68, 58)

        transaction = Transaction()

        # update columns
        transaction.add_query(self.query.update, self.table, 3, None, 95, None, None, 65)
        transaction.abort()

        # check to ese that original values are restored
        result = self.query.select(3, 0, [1, 1, 1, 1, 1])[0].columns
        self.assertEqual(result, [3, 88, 78, 68, 58])

    def test_abort_multiple_operations(self):
        """Test rollback of multiple operations in a single transaction."""
        # insert og records
        self.query.insert(4, 70, 60, 50, 40)
        self.query.insert(5, 90, 80, 70, 60)

        transaction = Transaction()
        transaction.add_query(self.query.update, self.table, 4, None, 95, None, None, 65)
        transaction.add_query(self.query.delete, self.table, 5)
        transaction.abort() 

        # check that record 4 contains original values
        result = self.query.select(4, 0, [1, 1, 1, 1, 1])[0].columns
        self.assertEqual(result, [4, 70, 60, 50, 40])

        # check that record 5 has not been deleted
        result = self.query.select(5, 0, [1, 1, 1, 1, 1])
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()
