from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self, lock_manager = None):
        self.queries = []
        self.log = []
        self.tid = id(self)  # Use object ID as transaction ID
        self.lock_manager = lock_manager
        self.locked_resources = set()  # Track what we've locked

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
        try:
            for query, table, args in self.queries:
                self.__acquireLocks(query, table, args)
                result = query(*args)
                if result == False:
                    return self.abort()
            
            return self.commit()
        
        except Exception as e:
            print(f"Transaction {self.tid} failed: {e}")
            return self.abort()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations

        if self.lock_manager:
            self.lock_manager.release_all_locks(self.tid)

        return False

    
    def commit(self):
        # TODO: commit to database
        if self.lock_manager:
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
                if hasattr(query, '__name__') and query.__name__ == 'select':
                    key = args[0]
                    rid_list = table.index.locate(table.key, key)
                    
                    if rid_list is not None:
                        for rid in rid_list:
                            self.lock_manager.acquire_lock(self.tid, rid, 'S')

            ## Write         
            else:  
                if hasattr(query, '__name__'):
                    if query.__name__ == 'insert':
                        pass

                    elif query.__name__ in ['update', 'delete']:
                        key = args[0]
                        rid_list = table.index.locate(table.key, key)
                        
                        if rid_list is not None:
                            for rid in rid_list:
                                self.lock_manager.acquire_lock(self.tid, rid, 'X')
                                
        except Exception as e:
            print(f"Lock acquisition failed: {e}")
            raise e

    
    def __determineOperation(self, query):
        if hasattr(query, '__name__'):
            if query.__name__ == 'select':
                return 'read'

            else:
                return 'write'
            
        return 'write'