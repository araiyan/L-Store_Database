from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager
import threading
import logging

class TransactionWorker:
    
    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None, lock_manager = None, deadlock_detection = True):
        self.stats = []
        self.transactions = transactions if transactions is not None else []
        self.result = 0
        self.worker_thread = None
        self.lock_manager = lock_manager if lock_manager is not None else LockManager()        
        self.deadlock_detection = deadlock_detection
        self.lock = threading.Lock()
        self.transaction_errors = {}
        self.timeout = 1000000              # idk what timeout value should be yet


    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        with self.lock:
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
        transactions_to_run = []
        
        with self.lock:
            transactions_to_run = list(self.transactions)
        
        for transaction in transactions_to_run:
            transaction_id = id(transaction)
            
            try:

                self.lock_manager.transaction_states[transaction_id] = 'growing'                
                result = transaction.run()
                self.stats.append(result)
                
                if not result:
                    self.transaction_errors[transaction_id] = "Transaction aborted"
                
            except Exception as e:

                logging.error(f"Transaction {transaction_id} failed with error: {str(e)}")
                self.stats.append(False)
                self.transaction_errors[transaction_id] = str(e)
                self.lock_manager.release_all_locks(transaction_id)

        self.result = len(list(filter(lambda x: x, self.stats)))