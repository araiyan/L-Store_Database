from lstore.table import Table

class Database():

    def __init__(self):
        self.tables:dict = {}
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if self.tables.get(name) is not None:
            raise NameError(f"Error creating Table! Following table already exists: {name}")
        
        self.tables[name] = Table(name, num_columns, key_index)
        return self.tables[name]

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if self.tables.get(name) is None:
            raise NameError(f"Error dropping Table! Following table does not exist: {name}")
        
        del self.tables[name]

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if self.tables.get(name) is None:
            raise NameError(f"Error getting Table! Following table does not exist: {name}")
        
        return self.tables[name]
