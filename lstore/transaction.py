from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager
from lstore.query import Query
from lstore.transaction_log import TransactionLog
import threading

#thread_local_data = threading.local()

class Transaction:
    
    log_manager = TransactionLog()

    """
    # Creates a transaction object.
    """
    def __init__(self, lock_manager=None):
        self.queries = []
        self.transaction_id = id(self)
        self.rollback_data = {}
        self.acquired_locks = []
        self.lock_manager = LockManager()  
        self.log_manager = TransactionLog()
        self.thread_data = threading.local()  #Store transaction state in thread-local storage


    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    """
     Executes the transaction:
    - Acquires necessary locks
    - Executes queries
    - Rolls back if any query fails
    - Commits if all succeed
    """
    def run(self):
        """Executes the transaction while ensuring atomicity and isolation."""
        self.rollback_data.clear()
        self.acquired_locks.clear()
        self.thread_data.active_transaction = self.transaction_id
        
        try:
            for query, table, args in self.queries:
                 # set table and record lock types
                if query.__name__ in ["select", "select_version", "sum", "sum_version"]:
                    table_lock_type = "IS"
                    record_lock_type = "S"
                else: # update, delete, insert
                    table_lock_type = "IX"
                    record_lock_type = "X"
                    
                key = args[0] if args else None
                self.lock_manager = table.lock_manager 
                # Acquire necessary locks
                if key is not None:
                    print(f"Transaction {self.transaction_id} trying to acquire lock on {key}...")
                    if not self.lock_manager.acquire_lock(self.transaction_id, key, record_lock_type):  
                        print(f"Transaction {self.transaction_id} failed to acquire lock on {key}, aborting.")
                        return self.abort()
                    print(f"Transaction {self.transaction_id} acquired lock on {key}.")
                    self.acquired_locks.append(key)
                                    
                result = query(*args, transaction_id=self.transaction_id)
                
                if not result:
                    print(f"Transaction {self.transaction_id} encountered an error, aborting.")
                    return self.abort()
                
                # Log after state
                before_state='none'
                after_state='none'
                #after_state = query_obj.select(key, table.key, [1] * table.num_columns) if key else None
                self.log_manager.log_operation(self.transaction_id, query.__name__, key, before_state, after_state)
                
            return self.commit()
        
        except Exception as e:
            print(f"Transaction {self.transaction_id} failed: {e}")
            return self.abort()
        
        finally:
            # 🔹 Remove transaction state after it finishes
            if hasattr(self.thread_data, "active_transaction"):
                del self.thread_data.active_transaction

    """
    Rolls back all executed operations and releases locks.
    Calls `rollback_update()` or `rollback_delete()` to restore previous values.
    """ 
    def abort(self):
        """ Rolls back a transaction using the log and releases all locks."""
        transaction_log_entries = self.log_manager.get_transaction_log(self.transaction_id)
        for entry in reversed(transaction_log_entries):
            table = entry['table']
            if entry['operation'] == 'update':
                table.update_record(entry['record_id'], entry['before'])
            elif entry['operation'] == 'delete':
                table.insert(*entry['before'])
            elif entry['operation'] == 'insert':
                table.delete(entry['record_id'])
        
        if hasattr(self, "lock_manager") and self.lock_manager:
            self.lock_manager.release_all_locks(self.transaction_id)
        self.log_manager.remove_transaction(self.transaction_id)
        print(f"Transaction {self.transaction_id} aborted and rolled back.")
        return False

    
    def commit(self):
        """ Commits the transaction and releases all acquired locks."""
        self.lock_manager.release_all_locks(self.transaction_id)  
        self.log_manager.remove_transaction(self.transaction_id)
        print(f"Transaction {self.transaction_id} committed successfully.")
        
        return True



