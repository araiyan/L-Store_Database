import sys
sys.path.append("..")

import unittest
from lstore.db import Database
from lstore.transaction import Transaction
from lstore.query import Query

class TestLocking(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up the database and create a single shared table for all tests."""
        cls.db = Database(path="test_db")
        cls.db.open("test_db")
        # Use a fixed table name instead of a random one.
        cls.table_name = "test_table"
        cls.table = cls.db.create_table(cls.table_name, 5, 0)

    @classmethod
    def tearDownClass(cls):
        """Clean up the shared table and close the database once after all tests."""
        try:
            cls.db.drop_table(cls.table_name)
        except Exception:
            pass
        cls.db.close()

    def test_single_transaction_read_write(self):
        q = Query(self.table)
        t = Transaction(lock_manager=self.db.lock_manager)
        # Insert a record with key 123 and then read it back.
        t.add_query(q.insert, self.table, 123, 1, 2, 3, 4)
        t.add_query(q.select, self.table, 123, 0, [1,1,1,1,1])
        self.assertTrue(t.run())

    def test_two_transactions(self):
        q = Query(self.table)
        # Transaction 1: Insert a record with key 456.
        t1 = Transaction(lock_manager=self.db.lock_manager)
        t1.add_query(q.insert, self.table, 456, 5, 6, 7, 8)
        
        # Transaction 2: Insert a different record with key 789.
        t2 = Transaction(lock_manager=self.db.lock_manager)
        t2.add_query(q.insert, self.table, 789, 9, 10, 11, 12)
        
        # Execute the transactions sequentially.
        self.assertTrue(t1.run())
        self.assertTrue(t2.run())
        
        # Verify that both records were inserted.
        t3 = Transaction(lock_manager=self.db.lock_manager)
        t3.add_query(q.select, self.table, 456, 0, [1,1,1,1,1])
        t3.add_query(q.select, self.table, 789, 0, [1,1,1,1,1])
        self.assertTrue(t3.run())

if __name__ == '__main__':
    unittest.main()
