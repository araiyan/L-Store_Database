from lstore.table import Table, Record
from lstore.index import Index

class Transaction:
    """
    A one-time-use transaction object that integrates strict 2PL.
    Locks are acquired before each query executes and are released only on commit or abort.
    """
    def __init__(self):
        self.queries = []
        self.tid = id(self)
        self.lock_manager = None
        self.completed = False 

    def add_query(self, query, table, *args):
        if self.completed:
            raise Exception("Cannot add query; this transaction is already complete.")
        self.queries.append((query, table, args))

    def run(self):
        if self.completed:
            raise Exception("This transaction has already been run.")
        success = True

        for query, table, args in self.queries:
            if self.lock_manager is None:
                self.lock_manager = table.lock_manager
                self.lock_manager.transaction_states[self.tid] = 'growing'

            try:
                self.__acquireLocks(query, table, args)

            except Exception as e:
                print(f"Transaction {self.tid} aborting due to lock acquisition error: {e}")
                success = False
                break

            result = query(*args)
            if result is False:
                print(f"Transaction {self.tid} aborting due to query failure.")
                success = False
                break

        if success:
            result = self.commit()

        else:
            result = self.abort()

        self.completed = True

        return result

    def abort(self):
        if self.lock_manager is not None:
            self.lock_manager.release_all_locks(self.tid)
        return False

    def commit(self):
        if self.lock_manager is not None:
            self.lock_manager.release_all_locks(self.tid)
        return True

    def __acquireLocks(self, query, table, args):
        operation_type = self.__determineOperation(query)
        try:
            
            # DB
            db_lock_type = 'IS' if operation_type == 'read' else 'IX'
            self.lock_manager.acquire_lock(self.tid, 'DB', db_lock_type)

            # Table
            table_lock_type = 'IS' if operation_type == 'read' else 'IX'
            self.lock_manager.acquire_lock(self.tid, table.name, table_lock_type)

            # Record
            ## Read
            if operation_type == 'read':
                key = args[0]
                rid_list = table.index.locate(table.key, key)
                if rid_list is not None:
                    for rid in rid_list:
                        self.lock_manager.acquire_lock(self.tid, rid, 'S')
            
            ## Write
            else:
                if hasattr(query, '__name__'):
                    if query.__name__ == 'insert':
                        # For insert, no record-level lock is needed since the record is new.
                        pass
                    
                    elif query.__name__ in ['update', 'delete']:
                        key = args[0]
                        rid_list = table.index.locate(table.key, key)
                        if rid_list is not None:
                            for rid in rid_list:
                                self.lock_manager.acquire_lock(self.tid, rid, 'X')
        except Exception as e:
            print(f"Lock acquisition failed for transaction {self.tid}: {e}")
            raise e

    def __determineOperation(self, query):
        if hasattr(query, '__name__') and query.__name__ == 'select':
            return 'read'
        else:
            return 'write'
