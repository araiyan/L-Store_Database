import sys

sys.path.append("..")
import unittest
import random
import time
from lstore.transaction_worker import TransactionWorker

class RandomDummyTransaction:

    def __init__(self, tid):
        self.tid = tid
        self.should_commit = random.choice([True, False])
        self.sleep_time = random.uniform(0.01, 0.1)
    
    def run(self):

        time.sleep(self.sleep_time)
        return self.should_commit

class TestTransactionWorkerConcurrency(unittest.TestCase):

    def test_concurrent_workers(self):

        num_workers = 6
        transactions_per_worker = 40
        
        workers = []
        expected_total_commits = 0
        
        # Create multiple workers, each w/  own set of random transactions
        for w in range(num_workers):
            worker = TransactionWorker()

            for t in range(transactions_per_worker):
                transaction = RandomDummyTransaction(t)
                expected_total_commits += int(transaction.should_commit)
                worker.add_transaction(transaction)
                
            workers.append(worker)
        
        # Start all workers
        for worker in workers:
            worker.run()

        for worker in workers:
            worker.join()
        
        # Aggregate results from all workers
        total_commits = sum(worker.result for worker in workers)
        print(f"Expected commits: {expected_total_commits}, Total commits: {total_commits}")
        self.assertEqual(total_commits, expected_total_commits)

if __name__ == '__main__':
    unittest.main()
