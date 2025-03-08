from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.log = []
        self.lock_managers = {}

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
    def run(self):
        '''Acquires intention/record locks and handles logging'''

        if not self.queries:
            return False
        
        # get transaction id, unique for every Transaction object
        transaction_id = id(self)
        locked_records_rid = set()
        locked_records_pk = set()
        locked_tables = set()


        for query, table, args in self.queries:
            
            # add lock manager (based on table) if not found, otherwise set lock_manager
            if table.name not in self.lock_managers:
                self.lock_managers[table.name] = table.lock_manager
            
            lock_manager = self.lock_managers[table.name]

            # set table and record lock types
            if query.__name__ in ["select", "select_version", "sum", "sum_version"]:
                table_lock_type = "IS"
                record_lock_type = "S"
            else: # update, delete, insert
                table_lock_type = "IX"
                record_lock_type = "X"

            # acquire lock on table if not present
            if table.name not in locked_tables:
                try: 
                    lock_manager.acquire_lock(transaction_id, table.name, table_lock_type)
                    locked_tables.add(table.name)
                except Exception:
                    # raise ValueError(f"Transaction {transaction_id} failed to acquire lock: {e}")
                    return self.abort()
            
            # handle record level locks; for select-adjacent queries, locks search through indexes
            if query.__name__ in ["select", "select_version", "sum", "sum_version"]:
                rids = table.index.locate(table.key, args[0])
                if rids is None:
                    continue
                
                for rid in rids:
                    if rid not in locked_records_rid:
                        try:
                            lock_manager.acquire_lock(transaction_id, rid, record_lock_type)
                            locked_records_rid.add(rid)
                        except Exception:
                            return self.abort()
                        
            # otherwise lock based on primary key
            else:
                primary_key = args[0]

                if primary_key not in locked_records_pk:
                    try:
                        lock_manager.acquire_lock(transaction_id, primary_key, record_lock_type)
                        locked_records_pk.add(primary_key)
                    except Exception:
                        return self.abort()


        # exectue queries and handle logging
        for query, table, args in self.queries:

            # create log dictionary to store changes made during a given transaction
            log_entry = {"query": query.__name__, "table": table, "args": args, "changes": []}

            # pass log_entry into query only if insert, update, or delete
            if query.__name__ in ["insert", "update", "delete"]:
                result = query(*args, log_entry=log_entry)
            else:
                result = query(*args) 
            
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            
            # log successful operations for future potential rollback
            self.log.append(log_entry)

        return self.commit()

    
    def abort(self):
        '''Rolls back the transaction and releases all locks'''

        #TODO: do roll-back and any other necessary operations
        return False

    
    def commit(self):
        '''Commits the transaction and releases all locks'''

        transaction_id = id(self)
   
        for lock_manager in self.lock_managers.values():
            lock_manager.release_all_locks(transaction_id)

        self.log.clear()
        return True

