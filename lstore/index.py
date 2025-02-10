"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from lstore.config import *
from BTrees.OOBTree import OOBTree

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        self.num_columns = table.num_columns
        self.key = table.key

        #only need primary index
        self.create_index(self.key)
        #for i in range(table.num_columns):
        #    self.create_index(i)

    """
    # returns the location of all records with the given value on column "column" as a list
    # returns None if no rid found
    """
    def locate(self, column, value):
        try:
            rid = self.indices[column][value]
            return list(rid)
        except KeyError:
            return None
       
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end" as a list
    # returns None if no rid found
    """
    def locate_range(self, begin, end, column):
        all_rid = list(self.indices[column].values(begin, end))

        #turn list of sets into just a list
        list_of_rid = []
        for rid in all_rid:
            list_of_rid.append(list(rid)[0])
        return list_of_rid

    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = OOBTree()
            return self.indices[column_number]
        else:
            return False

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        if self.indices[column_number]:
            self.indices[column_number].clear()
            self.indices[column_number] = None


    """
    # Search for values with RID in the parameter in indices
    """
    def search_value(self, column, rid):
        for key, value in column.items():
            if value == rid:
                return key
        return None

    """
    # Update new RID for all indices when a record is updated
    """
    def update_all_indices(self, primary_key, *columns):
        #get rid from primary key
        rid = self.indices[self.key][primary_key]

        #store original rid before it get changed 
        temp_rid = rid.copy()

        if not rid:
            return False
        for i in range(0, self.num_columns):
            if self.indices[i] != None:

                #search for value with matching rid
                value = self.search_value(self.indices[i], temp_rid)

                #remove rid from the set
                self.indices[i][value].discard(list(rid)[0])

                #for updated column, update both value and rid
                if columns[NUM_HIDDEN_COLUMNS + i] != None:

                    # if updated value is already in hash table, add updated rid to it, 
                    # if not, create a set for updated value and add updated rid
                    if columns[NUM_HIDDEN_COLUMNS + i] in self.indices[i]:
                        self.indices[i][columns[NUM_HIDDEN_COLUMNS + i]].add(columns[RID_COLUMN])
                    else:
                        self.indices[i][columns[NUM_HIDDEN_COLUMNS + i]] = set()
                        self.indices[i][columns[NUM_HIDDEN_COLUMNS + i]].add(columns[RID_COLUMN])

                # for columns not updated, only update rid
                else:
                    self.indices[i][value].add(columns[RID_COLUMN])

                #if previous value became an empty set bc of old deleted rid, remove it from hash table
                if self.indices[i][value] == set():
                        del self.indices[i][value]
        return True
    
    """
    # Takes a record and insert it into every indices
    # Columns are inserted in a tuple(rid, column value)
    """
    def insert_in_all_indices(self, *columns):
        #check for duplicated rid
        rid = self.locate(self.key, columns[self.key + NUM_HIDDEN_COLUMNS])
        if rid != None:
            return False
        for i in range(NUM_HIDDEN_COLUMNS, len(columns)):
            if self.indices[i - NUM_HIDDEN_COLUMNS] != None:

                #if column value is already in hash table, append rid, if not create new mapping 
                #for column value
                if columns[i] in self.indices[i - NUM_HIDDEN_COLUMNS]:
                    self.indices[i - NUM_HIDDEN_COLUMNS][columns[i]].add(columns[RID_COLUMN])
                else:
                    self.indices[i - NUM_HIDDEN_COLUMNS][columns[i]] = set()
                    self.indices[i - NUM_HIDDEN_COLUMNS][columns[i]].add(columns[RID_COLUMN])
        return True

    """
    # Remove element associated with rid : primary key from all indices
    """
    def delete_from_all_indices(self, primary_key):
        rid = self.indices[self.key][primary_key]
        if rid != None:
            return False
        for i in range(0, self.num_columns):
            if self.indices[i] != None:
                # look for value matched to rid
                value = self.search_value(self.indices[i], rid)

                #discard rid from the set, if it became empty set, delete the value too
                self.indices[i][value].discard(list(rid)[0])
                if self.indices[i][value] == set():
                    del self.indices[i][value]
        return True