from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager
import threading
import logging

class TransactionWorker:
    
    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None, lock_manager = None):
        self.stats = []
        self.transactions = transactions if transactions is not None else []
        self.result = 0
        self.worker_thread = None
        self.lock = threading.Lock()
        self.lock_manager = lock_manager
        self.transaction_errors = {}


    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        t.lock_manager = self.lock_manager
        self.transactions.append(t)
    
    """
    Runs all transaction as a thread
    """
    def run(self):
        self.worker_thread = threading.Thread(target = self.__run)
        self.worker_thread.start()
    
    """
    Waits for the worker to finish
    """
    def join(self):
        if self.worker_thread is not None:
            self.worker_thread.join()

        return self.result
    
    def __run(self):

        transactions_to_run = list(self.transactions) if len(self.transactions) > 0 else []
        print("Running transactions")
        
        for transaction in transactions_to_run:
            transaction_id = id(transaction)
            result = None
            
            try:
                result = transaction.run()

                self.stats.append(result)
                                
            except Exception as e:

                print(f"Transaction {transaction_id} failed with error: {str(e)}")
                self.stats.append(False)
                self.transaction_errors[transaction_id] = str(e)

        self.result = len(list(filter(lambda x: x, self.stats)))