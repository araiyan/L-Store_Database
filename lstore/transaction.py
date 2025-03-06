from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
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
        self.rollback_data = {}  # Reset rollback tracking
        self.successful_operations = 0
        
        for query, table, key, key_index, args in self.queries:
            try:
                if key is not None:
                    if not Transaction.lock_manager.acquire_lock(self.transaction_id, key, 'X'):
                        print(f"Transaction {self.transaction_id} failed to acquire lock on {key}, aborting.")
                        return self.abort()

                    self.acquired_locks.append(key)  # Track acquired lock

                # Handle updates
                if query.__name__ == "update":
                    prev_values = self.query_handler.update(key, *args)  
                    if prev_values:
                        if key not in self.rollback_data:
                            self.rollback_data[key] = []  # Initialize list
                        self.rollback_data[key].append(('update', prev_values))

                # Handle deletes
                elif query.__name__ == "delete":
                    prev_values = self.query_handler.delete(key)
                    if prev_values:
                        if key not in self.rollback_data:
                            self.rollback_data[key] = []
                        self.rollback_data[key].append(('delete', prev_values))

                else:
                    result = query(*((key,) if key is not None else ()) + args)
                    if not result:
                        print(f"Transaction {self.transaction_id} encountered an error, aborting.")
                        return self.abort()

                self.successful_operations += 1
            except Exception as e:
                print(f"Transaction {self.transaction_id} failed: {e}")
                return self.abort()

        return self.commit()
        
        
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    """
    Rolls back all executed operations and releases locks.
    Calls `rollback_update()` or `rollback_delete()` to restore previous values.
    """ 
    def abort(self):
        #log_entries = self.log_manager.get_transaction_log(self.transaction_id) //TBD to go thru log to reverse
        for rollback_key, prev_values_list in self.rollback_data.items():
            for prev_values in reversed(prev_values_list):
                self.query_handler.rollback_update(rollback_key, prev_values)  # Restore previous values

            Transaction.lock_manager.release_lock(self.transaction_id, "record", rollback_key, "X")

        # Release table-level locks
        Transaction.lock_manager.release_lock(self.transaction_id, "table", self.table.name, "IX")

        self.acquired_locks.clear()
        print(f"Transaction {self.transaction_id} aborted and rolled back.")
        return False

    
    def commit(self):
        """Commits the transaction and releases all acquired locks."""
        for key in self.acquired_locks:
            Transaction.lock_manager.release_lock(self.transaction_id, "record", key, "X")  # Release record-level locks

        # Release table-level locks
        Transaction.lock_manager.release_lock(self.transaction_id, "table", self.table.name, "IX")

        self.acquired_locks.clear()
        print(f"Transaction {self.transaction_id} committed successfully.")
        return True


