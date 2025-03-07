from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
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
        #TODO: do roll-back and any other necessary operations
        return False

    
    def commit(self):

        # clear log list after commiting changes
        self.log.clear()
        return True

