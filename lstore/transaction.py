from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager
from lstore.config import *
from lstore.query import Query
from lstore.transaction_log import TransactionLog

class Transaction:
    
    log_manager = TransactionLog()

    """
    # Creates a transaction object.
    """
    def __init__(self, lock_manager=None):
        self.queries = []
        self.transaction_id = id(self)
        # list of log dictionaries
        # Use a separate log manager for persistent logging.
        self.log_manager = TransactionLog()
        # Local undo log for rollback
        self.undo_log = []


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
        '''Acquires intention/record locks and handles logging'''
        transaction_id = id(self)
        locked_records = {}
        locked_tables = {}

        for query, table, args in self.queries:
            # set table and record lock types
            record_identifier = self.__query_unique_identifier(query, table, args)

            if record_identifier not in locked_records:
                if (query.__name__ in ["select", "select_version", "sum", "sum_version"]):
                    locked_records[record_identifier] = "S"
                    locked_tables[record_identifier] = "IS"
                    try:
                        table.lock_manager.acquire_lock(transaction_id, record_identifier, "S")
                        table.lock_manager.acquire_lock(transaction_id, table.name, "IS")
                    except Exception as e:
                        print("Failed to aquire shared lock: ", e)
                        return self.abort()
                    
                else:
                    locked_records[record_identifier] = "X"
                    locked_tables[record_identifier] = "IX"
                    try:
                        table.lock_manager.acquire_lock(transaction_id, record_identifier, "X")
                        table.lock_manager.acquire_lock(transaction_id, table.name, "IX")
                    except Exception as e:
                        print("Failed to aquire exclusive lock: ", e)
                        return self.abort()
                    
            elif (locked_records[record_identifier] == "S" and query.__name__ in ["update", "delete", "insert"]):
                locked_records[record_identifier] = "X"
                locked_tables[record_identifier] = "IX"
                try:
                    table.lock_manager.upgrade_lock(transaction_id, record_identifier, "S", "X")
                    table.lock_manager.upgrade_lock(transaction_id, table.name, "IS", "IX")
                except Exception as e:
                    print("Failed to upgrade lock: ", e)
                    return self.abort()

        # exectue queries and handle logging
        for query, table, args in self.queries:

            # create log dictionary to store changes made during a given transaction
            log_entry = {"query": query.__name__, "table": table, "args": args, "changes": []}

            # pass log_entry into query only if insert, update, or delete
            if query.__name__ in ["insert", "update", "delete"]:
                result = query(*args, log_entry=log_entry)

            elif query.__name__ in ["select", "select_version", "sum", "sum_version"]:
                result = query(*args) 
            
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            
            # Record the log entry locally for potential rollback and in the log manager 
            self.undo_log.append(log_entry)
            
            # Record the log entry locally and in the persistent log manager for recovery
            self.undo_log.append(log_entry)
            op = log_entry.get("query")
            rid = log_entry.get("rid")
            pre_args = log_entry.get("prev_columns")
            post_args = log_entry.get("columns")
            self.log_manager.log_operation(transaction_id, op, rid, pre_args, post_args)
            

        return self.commit()

    
    def abort(self):
        '''Rolls back the transaction and releases all locks'''

        # reverse logs to process latest first
        for entry in reversed(self.undo_log):

            query_type = entry["query"]
            table = entry["table"]
            args = entry["args"]
            changes = entry["changes"]
            
            primary_key = args[0]


            if query_type == "insert":
                # undo insert - remove inserted record from storage and indexes
                try:
                    # For an insert, rollback by deleting the inserted record.
                    rid = changes["rid"]
                    # soft delete
                    table.deallocation_base_rid_queue.put(rid)
                    table.index.delete_from_all_indices(primary_key)
                    table.index.delete_logged_columns()
                except Exception as e:
                    print(f"Error rolling back insert for RID {rid}: {e}")
 
            elif query_type == "delete":
                # undo delete - insert values into indexes

                try:
                    # For a delete, rollback by re-inserting the deleted record.                    
                    prev_columns = changes["prev_columns"]
                    rid = changes["rid"]

                    # create new record object based on logged values
                    record = Record(rid, primary_key, prev_columns)

                    # insert record and restore indexes
                    table.insert_record(record)
                    #table.index.clear_logged_columns()
                    table.index.insert_in_all_indices(record.columns)
                except Exception as e:
                    print(f"Error rolling back delete for RID {rid}: {e}")
                
            elif query_type == "update":
                # undo update - restore previous column values
                try:
                    # For an update, rollback by restoring the previous state.
                    prev_columns = changes["prev_columns"]  #new columns now when reversed
                    upd_columns = changes["columns"]  #prev columns now
                    table.index.delete_from_all_indices(prev_columns[table.key], prev_columns)
                    table.index.delete_logged_columns()
                    #table.index.update_all_indices(prev_columns[table.key], prev_columns, upd_columns)
                    table.update_record(rid, prev_columns)
                    table.index.update_all_indices(primary_key, prev_columns,upd_columns)
                except Exception as e:
                    print(f"Error rolling back update for RID {rid}: {e}")
        
        transaction_id = id(self)
        if self.queries and self.queries[0][1].lock_manager.transaction_states.get(transaction_id):
            self.queries[0][1].lock_manager.release_all_locks(transaction_id)
        return False

    
    def commit(self):
        '''Commits the transaction and releases all locks'''

        transaction_id = id(self)

        # release_all_locks called on one table since lock_manager is shared globally
        self.queries[0][1].lock_manager.release_all_locks(transaction_id)
        self.undo_log.clear()
        # TBD, persist the log to disk here.

        return True

    def __query_unique_identifier(self, query, table, args):
        if (query.__name__ in ["delete", "update"]):
            return (args[0], table.key)
        elif (query.__name__ == "insert"):
            return (args[table.key], table.key)
        elif (query.__name__ in ["select", "select_version"]):
            return (args[0], args[1])
        elif (query.__name__ in ["sum", "sum_version"]):
            return args[3]
        else:
            raise ValueError(f"Query {query.__name__} not supported")