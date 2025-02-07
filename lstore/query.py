from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import *


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
        try:
            # retrieve a list of RIDs that contain the "search_key" value within the column as defined by "search_key_index"
            rid_list = self.table.index.locate(search_key_index, search_key)

            # if there exists no RIDs that match the given parameters, return False
            if not rid_list:
                return False

            record_objs = []

            # iterate through all desired RIDs
            for rid in rid_list:
                base_page_location = self.table.page_directory.get(rid, None)
                if base_page_location is None:
                    continue
                    
                # store the (base) page# and index# that the RID/row is located
                base_page_number, base_page_index = base_page_location[0]

                # store base values within projected columns - we will update values with latest data as necessary 
                record_columns = [
                    self.table.base_pages[NUM_HIDDEN_COLUMNS + i][base_page_number].get(base_page_index)
                    for i in range(len(projected_columns_index)) if projected_columns_index[i] == 1
                ]

                # retrieve indirection RID so that we can traverse through updated pages/records
                indirection_rid = self.table.base_pages[INDIRECTION_COLUMN][base_page_number].get(base_page_index)

                while indirection_rid != rid:
                    # grab schema encoding value to determine whether or not a given column within a tail record was updated
                    schema_encoding = self.table.__grab_tail_value_from_rid(indirection_rid, SCHEMA_ENCODING_COLUMN)
                    
                    for i in range(len(projected_columns_index)):
                        if projected_columns_index[i] == 1 and ((schema_encoding >> i) & 1) == 1:
                            record_columns[i] = self.table.__grab_tail_value_from_rid(indirection_rid, NUM_HIDDEN_COLUMNS + i) 
                            projected_columns_index[i] = 0 # we no longer want to update this column - otherwise, we would be overriding newer data with old data

                    # update indirection_rid to the next (previous version) within chain of tail records
                    indirection_rid = self.table.__grab_tail_value_from_rid(indirection_rid, INDIRECTION_COLUMN)

                record_objs.append(Record(rid, search_key, record_columns))

            return record_objs if record_objs else False
        
        except Exception as e:
            print(f"Error in select: {e}")
            return False

    
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
        try:
            rid_list = self.table.index.locate(search_key_index, search_key)

            if not rid_list:
                return False
            
            record_objs = []

            for rid in rid_list:
                base_page_location = self.table.page_directory.get(rid, None)
                if base_page_location is None:
                    continue

                base_page_number, base_page_index = base_page_location[0]

                indirection_rid = self.table.base_pages[INDIRECTION_COLUMN][base_page_number].get(base_page_index)

                # traverse through the tail pages based on the relative version
                current_version = 0
                while current_version > relative_version and indirection_rid != rid:
                    current_version -= 1
                    indirection_rid = self.table.__grab_tail_value_from_rid(indirection_rid, INDIRECTION_COLUMN)
                
                # unsuccessful in finding an older version - this implies that indirection_rid == rid and we can
                # thus directly retrieve from the base page
                if current_version > relative_version:
                    record_columns = [
                    self.table.base_pages[NUM_HIDDEN_COLUMNS + i][base_page_number].get(base_page_index)
                    for i in range(len(projected_columns_index)) if projected_columns_index[i] == 1
                ]
                # otherwise, retrieve from the relative version
                else:
                    record_columns = [
                    self.table.__grab_tail_value_from_rid(indirection_rid, NUM_HIDDEN_COLUMNS + i)
                    if projected_columns_index[i] == 1 else None
                    for i in range(len(projected_columns_index))
                ]
                    
                record_objs.append(Record(rid, search_key, record_columns))
                
            return record_objs if record_objs else False
        
        except Exception as e:
            print(f"Error during select_version: {e}")
            return False

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        pass

    
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
