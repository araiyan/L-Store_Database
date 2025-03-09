from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager
from lstore.config import *

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []

        # list of log dictionaries
        self.log = []

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
                    table.lock_manager.acquire_lock(transaction_id, table.name, table_lock_type)
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
                            table.lock_manager.acquire_lock(transaction_id, rid, record_lock_type)
                            locked_records_rid.add(rid)
                        except Exception:
                            return self.abort()
                        
            # otherwise lock based on primary key (update, insert, delete)
            else:
                primary_key = args[0]

                if primary_key not in locked_records_pk:
                    try:
                        table.lock_manager.acquire_lock(transaction_id, primary_key, record_lock_type)
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

        # reverse logs to process latest first
        for entry in reversed(self.log):
            query_type = entry["query"]
            table = entry["table"]
            args = entry["args"]
            changes = entry["changes"]
            
            primary_key = args[0]


            if query_type == "insert":
                # undo insert - remove inserted record from storage and indexes

                rid = changes["rid"]
                # soft delete
                table.deallocation_base_rid_queue.put(rid)
                table.index.delete_from_all_indices(primary_key)
            elif query_type == "delete":
                # undo delete - insert values into indexes

                prev_columns = changes["prev_columns"]
                rid = changes["rid"]

                # create new record object based on logged values
                record = Record(rid, primary_key, prev_columns)

                # insert record and restore indexes
                table.insert_record(record)
                table.index.insert_in_all_indices(record.columns)
            elif query_type == "update":
                # undo update - restore previous column values

                prev_columns = changes["prev_columns"]

                table.index.update_all_indices(prev_columns[table.key], prev_columns, prev_columns)
        
        transaction_id = id(self)
        if self.queries and self.queries[0][1].lock_manager.transaction_states.get(transaction_id):
            self.queries[0][1].lock_manager.release_all_locks(transaction_id)
        return False

    
    def commit(self):
        '''Commits the transaction and releases all locks'''

        transaction_id = id(self)

        # release_all_locks called on one table since lock_manager is shared globally
        self.queries[0][1].lock_manager.release_all_locks(transaction_id)

        self.log.clear()
        return True

