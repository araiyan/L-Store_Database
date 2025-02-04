from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table:Table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        # Note: Ensure schema_encoding is converted to int before inserted into record.column
        pass

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        pass

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
        """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        
        # NOTE: NOT FOCUSING ON 2 PHASE LOCKING AS MILESTONE 1 REQUIRES ONLY A FUNCTIONING DB, NOT CONCURRENT

        # We want to see if the record exists with the specified key; no point in continuing further without checking
        rid_exists = self.table.index.locate(self.table.key, primary_key)
        if rid_exists is None:
            return False
        
        # Checking if we're passing the right number of values over to columns (one value per column, not more not less)
        if len(columns) != self.table.num_columns:
            return False

        
        # Creating new tail record 
        # =========================================================================================================================================
        # The tail should have the same number of columns as the other pages so we should be multiplying by the total_num_columns value
        # Multiplying by [None] atm since the columns don't have an assigned value / size (I think it works like this at least, should be similar to OS in that sense)
        new_columns = [None] * self.table.total_num_columns

        # Insert values into the new column
        for i, value in enumerate(columns):

            # If we're modifying the primary_key then this update should be stopped since we can't change the primary_key column
            if i == self.table.key and value != primary_key:
                return False
            
            # We want to skip past the hidden columns since those shouldn't be touched
            new_columns[NUM_HIDDEN_COLUMNS + i] = value

        # Finally makes a new record tail page using the values created in this function. Records class will be used to update in update_record()
        record = Record(rid = -1, key = primary_key, columns = new_columns)

        # Assigning tail record in table
        # =========================================================================================================================================
        # Assign RID for newly created tail record
        self.table.assign_rid_to_record(record)

        # Initialize record entry into directory
        self.table.page_directory[record.rid] = [None] * self.table.total_num_columns

        # Updates the record in the table
        self.table.update_record(record)

        # Update successful
        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
