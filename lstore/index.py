"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from lstore.config import *
from sortedcontainers import SortedList

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns # since we're not indexing hidden columns, unless?
        self.num_columns = table.num_columns
        self.key = table.key
        for i in range(table.num_columns):
            self.create_index(i)

    """
    # returns the location of all records with the given value on column "column" as a list
    # returns None if no rid found
    """
    def locate(self, column, value):
        return self.search_all(self.indices[column], value, value)
       
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end" as a list
    # returns None if no rid found
    """
    def locate_range(self, begin, end, column):
        return self.search_all(self.indices[column], begin, end) 

    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = SortedList([])
            return self.indices[column_number]
        else:
            raise ValueError(f"Cannot create index for column: {column_number}. Index already exists!")

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        if self.indices[column_number]:
            self.indices[column_number].clear()
            self.indices[column_number] = None

    """
    # Search through values in the 2 tuple and returns all rid corresponds to the value
    """
    def search_all(self, column, begin, end):
        low = 0
        high = len(column) - 1
        rids = []
        while low <= high:
            mid = low + (high - low) // 2
            if begin <= column[mid][1] <= end:
                temp_mid = mid + 1
                while mid >= low and begin <= column[mid][1]:
                    rids.append(column[mid][0])
                    mid -= 1
                while temp_mid <= high and column[temp_mid][1] <= end:
                    rids.append(column[temp_mid][0])
                    temp_mid += 1
                return rids
            elif column[mid][1] < begin:
                low = mid + 1
            elif column[mid][1] > end:
                high = mid - 1
        return None

    """
    # Search for values with RID in the parameter in indices
    """
    def search_value(self, column, rid):
        low = 0
        high = len(column) - 1
        while low <= high:
            mid = low + (high - low) // 2
            if rid == column[mid][0]:
                return column[mid][1]
            elif column[mid][0] < rid:
                low = mid + 1
            elif column[mid][0] > rid:
                high = mid - 1
        return None

    """
    # Update new RID for all indices when a record is updated
    """
    def update_all_indices(self, primary_key, *columns):
        rid = self.search_all(self.indices[self.key], primary_key, primary_key)
        if not rid:
            raise ValueError(f"Cannot update indices. Key: {primary_key} not found!")
        for i in range(0, self.num_columns):
            if self.indices[i] != None:
                value = self.search_value(self.indices[i], rid[0])
                self.indices[i].discard((rid[0], value))
                self.indices[i].add((columns[RID_COLUMN], columns[NUM_HIDDEN_COLUMNS + i]))
    
    """
    # Takes a record and insert it into every indices
    # Columns are inserted in a tuple(rid, column value)
    """
    def insert_in_all_indices(self, *columns):
        rid = self.search_all(self.indices[self.key], columns[NUM_HIDDEN_COLUMNS + self.key], columns[NUM_HIDDEN_COLUMNS + self.key])
        if rid != None:
            raise ValueError(f"Column with key: {columns[NUM_HIDDEN_COLUMNS + self.key]} already exists.")
        for i in range(NUM_HIDDEN_COLUMNS, len(columns)):
            if self.indices[i - NUM_HIDDEN_COLUMNS] != None:
                self.indices[i - NUM_HIDDEN_COLUMNS].add((columns[RID_COLUMN], columns[i]))

    """
    # Remove element associated with rid : primary key from all indices
    """
    def delete_from_all_indices(self, primary_key):
        rid = self.search_all(self.indices[self.key], primary_key, primary_key)
        if not rid:
            raise ValueError(f"Cannot delete from indices. Key: {primary_key} not found!")
        for i in range(0, self.num_columns):
            if self.indices[i] != None:
                value = self.search_value(self.indices[i], rid[0])
                self.indices[i].discard((rid[0], value))