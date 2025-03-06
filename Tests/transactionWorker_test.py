import sys

sys.path.append("..")
import unittest
import threading
import time
import os
import shutil

from lstore.db import Database
from lstore.table import Table
from lstore.query import Query
from lstore.transaction import Transaction

TEST_DB_PATH = "test_db_unit"

class TestBasicConcurrency(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print("\n--- Setting up DB ---")
        cls.db = Database()
        cls.db.open(TEST_DB_PATH)
        cls.table = cls.db.create_table("TestTable", 5, 0)

    @classmethod
    def tearDownClass(cls):
        print("\n--- Tearing down DB ---")
        cls.db.close()
        if os.path.exists(TEST_DB_PATH):
            shutil.rmtree(TEST_DB_PATH)
            print(f"Removed test database folder: {TEST_DB_PATH}")

    def test_concurrent_transactions(self):
        queryOp = Query(self.table)
        print("\n=== Insert Initial Records ===")
        initial_data = [
            [0, 100, 200, 300, 400],
            [1, 110, 210, 310, 410],
            [2, 120, 220, 320, 420],
            [3, 130, 230, 330, 430],
            [4, 140, 240, 340, 440],
        ]
        
        for row in initial_data:
            queryOp.insert(*row)
            print(f"<Inserted record> primaryKey = {row[0]}: {row}")

        print("\n=== Checking initial records via SELECT ===")
        
        for i in range(len(initial_data)):
            rec = queryOp.select(i, self.table.key, [1,1,1,1,1])
            
            if rec:
                print(f"  primaryKey = {i}: {rec[0].columns}")
            
            else:
                print(f"  primaryKey = {i}: Missing")

        print("\n=== Launching Worker Threads ===")
        NUM_THREADS = 3
        threads = []
        
        for t_id in range(NUM_THREADS):
            t = threading.Thread(target=self._worker_ops, args=(t_id,))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()

        print("\n=== All Threads Finished - Checking Final Results ===")
        missing_count = 0
        
        for i in range(len(initial_data)):
            rec = queryOp.select(i, self.table.key, [1,1,1,1,1])
            
            if not rec or len(rec) == 0:
                missing_count += 1
                print(f"  ERROR: primaryKey = {i}: Missing after concurrency")
            
            else:
                final_vals = rec[0].columns
                print(f"  Final primaryKey = {i}: {final_vals}")
        
        self.assertEqual(missing_count, 0, f"Missing {missing_count} records out of {len(initial_data)}.")

    def _worker_ops(self, worker_id):
        
        queryOp = Query(self.table)
        transaction = Transaction()
        primaryKey_to_update = worker_id % 5
        current_rec = queryOp.select(primaryKey_to_update, self.table.key, [1,1,1,1,1])
        
        if current_rec:
            old_col2 = current_rec[0].columns[2]
            print(f"[Thread {worker_id}] primaryKey = {primaryKey_to_update}, old col[2]: {old_col2}")
        
        else:
            print(f"[Thread {worker_id}] primaryKey = {primaryKey_to_update} is missing on select!")
            old_col2 = None
        
        
        updated_cols = [None] * 5
        updated_cols[2] = 9000 + worker_id
        transaction.add_query(queryOp.update, self.table, primaryKey_to_update, *updated_cols)
        print(f"[Thread {worker_id}] is updating primaryKey: {primaryKey_to_update} => col[2] = {updated_cols[2]}")
        transaction.add_query(queryOp.increment, self.table, primaryKey_to_update, 3)
        
        
        def partial_sum_print():
            s = queryOp.sum(0, 4, 2)
            print(f"[Thread {worker_id}] partial sum of col[2]: {s}")
        
        transaction.add_query(lambda: partial_sum_print(), self.table)
        success = transaction.run()
        
        if success:
            print(f"[Thread {worker_id}] COMMITTED primaryKey = {primaryKey_to_update}")
        
        else:
            print(f"[Thread {worker_id}] ABORTED primaryKey = {primaryKey_to_update}")
        
        time.sleep(0.02)

if __name__ == "__main__":
    unittest.main()
