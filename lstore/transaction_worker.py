from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager
import threading
import logging

class TransactionWorker:
    
    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None):
        self.stats = []
        self.transactions = transactions if transactions is not None else []
        self.result = 0
        self.worker_thread = None
        self.lock = threading.Lock()


    """
    Appends t to transactions
    """
    def add_transaction(self, t):
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
        
        for transaction in transactions_to_run:
            result = None
            
            result = transaction.run()
            self.stats.append(result)

        self.result = len(list(filter(lambda x: x, self.stats)))