"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from lstore.config import *
from sortedcontainers import SortedList

from BTrees.OOBTree import OOBTree

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        # indices that maps primary key to updated column values
        self.value_mapper = [None] * table.num_columns

        self.num_columns = table.num_columns
        self.key = table.key

        self.create_index(self.key)

    """
    # returns the location of all records with the given value on column "column" as a list
    # returns None if no rid found
    """
    def locate(self, column, value):
        return list(self.indices[column].get(value, [])) or None
       
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end" as a list
    # returns None if no rid found
    """
    def locate_range(self, begin, end, column):
        return list(self.indices[column].values(min=begin, max=end)) or None

    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = OOBTree()
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
    # Update new RID for all indices when a record is updated
    """
    def update_all_indices(self, primary_key, prev_columns, *columns):
        rid = self.locate(self.key, primary_key)
        if not rid:
            raise ValueError(f"Cannot update indices. Key: {primary_key} not found!")
        for i in range(0, self.num_columns):
            if self.indices[i] != None and columns[NUM_HIDDEN_COLUMNS + i] != None:
                value = prev_columns[i]
                del self.indices[i][value][rid[0]]
                self.indices[i][columns[NUM_HIDDEN_COLUMNS + i]][rid[0]] = True

    def insert_to_index(self, column_index, column_value, rid):
        '''Used to insert the primary key for primary key indexing'''
        index:OOBTree = self.indices[column_index]


        if (index is None):
            raise IndexError("No indicy in the specified column")
        
        if (not index.get(column_value)):
            index[column_value] = {}

        index[column_value][rid] = True
    
    """
    # Takes a record and insert it into every indices
    # Columns are inserted in a tuple(rid, column value)
    """
    def insert_in_all_indices(self, *columns):
        rid = self.locate(self.key, columns[NUM_HIDDEN_COLUMNS + self.key])
        if rid != None:
            raise ValueError(f"Column with key: {columns[NUM_HIDDEN_COLUMNS + self.key]} already exists.")
        
        recird_rid = columns[RID_COLUMN]
        for i in range(NUM_HIDDEN_COLUMNS, len(columns)):
            if self.indices[i - NUM_HIDDEN_COLUMNS] != None:
                self.indices[i - NUM_HIDDEN_COLUMNS][columns[i]][recird_rid] = True

    """
    # Remove element associated with rid : primary key from all indices
    """
    def delete_from_all_indices(self, *columns):
        rid = self.locate(self.key, columns[NUM_HIDDEN_COLUMNS + self.key])
        if not rid:
            raise ValueError(f"Cannot delete from indices. Key: {columns[NUM_HIDDEN_COLUMNS + self.key]} not found!")
        for i in range(0, self.num_columns):
            if self.indices[i] != None:
                value = columns[NUM_HIDDEN_COLUMNS + i]
                del self.indices[i][value][rid[0]]