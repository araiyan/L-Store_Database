import sys
sys.path.append("..")

import unittest
import tempfile
import os
import random
from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker


"""
Basically, we want to test for concurrency and proper locking in 2PLCC_test.py

Just make some transactions to perform on query (insert, update, select) and hope they're right lol 
(I have no idea what I'm doing at this point I hate concurrency)

Haven't implemented a test for deleting records so uhhh yeah
"""

class TestConcurrentTransactions(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = self.temp_dir.name
        print(f"Setting up database at {self.db_path}")
        self.db = Database()
        self.db.open(self.db_path)
        self.table = self.db.create_table("ConcurrentTest", 5, 0)
        self.query = Query(self.table)
        print("Created table 'ConcurrentTest' with 5 columns.")

    def tearDown(self):
        print("Tearing down database.")
        self.db.close()
        self.temp_dir.cleanup()

    def test_concurrent_inserts_updates_selects(self):
        number_of_transactions = 50
        number_of_threads = 5

        # Insert
        print("\n=== Inserting records concurrently ===")
        insert_transactions = []
        
        for i in range(number_of_transactions):
            key = 1000 + i
            
            record = [key, random.randint(0, 100), random.randint(0, 100), random.randint(0, 100), random.randint(0, 100)]
            
            t = Transaction()
            t.add_query(self.query.insert, self.table, *record)
            insert_transactions.append(t)
            
            print(f"  Created Insert Transaction {t.tid} for key {key}")

        # Distribute insert transactions
        insert_workers = [TransactionWorker() for _ in range(number_of_threads)]
        for idx, transaction in enumerate(insert_transactions):
            worker_index = idx % number_of_threads
            insert_workers[worker_index].add_transaction(transaction)
            print(f"  Assigned Insert Transaction {transaction.tid} to Worker {worker_index}")

        # Worker threads for insert operation
        for i, worker in enumerate(insert_workers):
            print(f"  Starting Insert Worker {i}")
            worker.run()

        for i, worker in enumerate(insert_workers):
            result = worker.join()
            print(f"  Insert Worker {i} finished with {result} committed transactions.")

        # Update
        print("\n=== Updating records concurrently ===")
        update_transactions = []
        new_value = 999  # Testing for concurrent updates so assigning a new value to col[1]

        for i in range(number_of_transactions):
            
            key = 1000 + i
            update_values = [None, new_value, None, None, None]
            
            # Add update operation to transactions
            t = Transaction()
            t.add_query(self.query.update, self.table, key, *update_values)
            update_transactions.append(t)
            
            print(f"  Created Update Transaction {t.tid} for key {key} setting column 1 to {new_value}")

        # Distribute update transactions
        update_workers = [TransactionWorker() for _ in range(number_of_threads)]
        for idx, transaction in enumerate(update_transactions):
            worker_index = idx % number_of_threads
            update_workers[worker_index].add_transaction(transaction)
            print(f"  Assigned Update Transaction {transaction.tid} to Worker {worker_index}")

        # Worker threads for update operation
        for i, worker in enumerate(update_workers):
            print(f"  Starting Update Worker {i}")
            worker.run()

        for i, worker in enumerate(update_workers):
            result = worker.join()
            print(f"  Update Worker {i} finished with {result} committed transactions.")

        print("\n=== Verifying updated records with concurrent selects ===")
        select_workers = [TransactionWorker() for _ in range(number_of_threads)]
        select_transactions = []

        for i in range(number_of_transactions):
            key = 1000 + i
            
            t = Transaction()
            t.add_query(self.query.select, self.table, key, 0, [1, 1, 1, 1, 1])
            select_transactions.append((key, t))
            
            worker_index = i % number_of_threads
            select_workers[worker_index].add_transaction(t)
            
            print(f"  Assigned Select Transaction {t.tid} for key {key} to Worker {worker_index}")

        # Worker threads for select operation
        for i, worker in enumerate(select_workers):
            print(f"  Starting Select Worker {i}")
            worker.run()

        for i, worker in enumerate(select_workers):
            result = worker.join()
            print(f"  Select Worker {i} finished with {result} committed transactions.")

        print("\n=== Final Verification of Records ===")
        for i in range(number_of_transactions):
            key = 1000 + i
            results = self.query.select(key, 0, [1, 1, 1, 1, 1])

            if results and len(results) >= 1:
                rec = results[0]
                print(f"  Record with key {key} found; column 1 value: {rec.columns[1]}")
                self.assertEqual(rec.columns[1], new_value, f"Update failed for key {key}")

            else:
                print(f"  ERROR: Record with key {key} NOT found!")
                self.fail(f"Record with key {key} not found.")

if __name__ == '__main__':
    unittest.main()
