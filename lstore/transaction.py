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
                table.index.delete_logged_columns()
            elif query_type == "delete":
                # undo delete - insert values into indexes

                prev_columns = changes["prev_columns"]
                rid = changes["rid"]

                # create new record object based on logged values
                record = Record(rid, primary_key, prev_columns)

                # insert record and restore indexes
                table.insert_record(record)
                table.index.clear_logged_columns()
                #table.index.insert_in_all_indices(record.columns)
            elif query_type == "update":
                # undo update - restore previous column values

                prev_columns = changes["prev_columns"]
                table.index.delete_from_all_indices(prev_columns[table.key], prev_columns)
                table.index.delete_logged_columns()
                #table.index.update_all_indices(prev_columns[table.key], prev_columns, prev_columns)
        
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