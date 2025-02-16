"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from lstore.config import *
from BTrees.OOBTree import OOBTree
import pickle

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        # maps primary key to its most updated column values

        """
        self.value_mapper[primary_key] = [column1_value, column2_value...]
        """
        self.value_mapper = OOBTree()
        self.secondary_index = {}

        self.num_columns = table.num_columns
        self.key = table.key

        self.indices[self.key] = OOBTree()
        #self.table = table

    """
    # returns the location of all records with the given value on column "column" as a list
    # returns None if no rid found
    """

    #only use this to find rid when given primary key
    def locate(self, column, value):

        #deserialize

        if value in self.indices[column]:
            rids = list(self.indices[column][value].keys())
        else:
            rids = []

        #serialize
        return rids
    
    def get(self, key_column_number, value_column_number, key):

        #deserialize

        if (key_column_number, value_column_number) in self.secondary_index:
            values = list(self.secondary_index[key_column_number, value_column_number][key])

            #serialize

            return values
        else:
            values = []
            for primary_key, column_values in self.value_mapper.items():

                # get key and values from value mapper
                value_mapper_key = column_values[key_column_number]
                value_mapper_value = column_values[value_column_number]
                if value_mapper_key == key:
                    values.append(value_mapper_value)
            
            #serialize

            return values

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end" as a list
    # returns None if no rid found
    """

    #only use for keys

    def locate_range(self, begin, end, column):

        #deserialize

        rids = [rid for sublist in self.indices[column].values(min=begin, max=end) for rid in sublist.keys()] or None

        #serialize
        return rids

    """
    # optional: Create index on specific columns 
    # index will have key and column mapped as {key_column_value1: [value_column_value1]}
    # or {key_column_value1: [value_column_value1, value_column_value2...]} if more than one value maps to the same key
    """
    def create_index(self, key_column_number, value_column_number):

        #deserialize

        if (key_column_number, value_column_number) not in self.secondary_index:

            btree = OOBTree()

            # go through value mapper to create new index
            for primary_key, column_values in self.value_mapper.items():

                # get key and values from value mapper
                key = column_values[key_column_number]
                value = column_values[value_column_number]

                # if key already is in btree, append it
                if btree.get(key):
                    btree[key].append(value)
                else:
                    # if not then create new entry
                    btree[key] = [value]

            self.secondary_index[(key_column_number, value_column_number)] = btree

            #serialize

            return self.secondary_index[(key_column_number, value_column_number)]
        else:
            raise IndexError(f"Index with key column: {key_column_number} and value column: {value_column_number} already exists.")


    """
    # optional: Drop index of specific column
    """
    def drop_index(self, key_column_number, value_column_number):

        #deserialize

        # clears Btree and removes reference 
        if self.secondary_index[(key_column_number, value_column_number)] != None:

            self.secondary_index[(key_column_number, value_column_number)].clear()
            del self.secondary_index[(key_column_number, value_column_number)]

        #somehow remove the index file from disk

        # if the index trying to drop doesn't exist in the dict of secondary indices
        else:
            raise IndexError(f"Index with key column: {key_column_number} and value column: {value_column_number} does not exist.")

    
    def delete_from_index(self, column_index, column_value):
        '''Used to delete a single value from an index'''
        index:OOBTree = self.indices[column_index]


        if (index is None):
            raise IndexError("No indicy in the specified column")
        

        if (index.get(column_value)):
            del index[column_value]
        else:
            raise ValueError("Value not found in index")

    def insert_to_index(self, column_index, column_value, rid):
        '''Used to insert the primary key for primary key indexing'''
        index:OOBTree = self.indices[column_index]

        if (index is None):
            raise IndexError("No indicy in the specified column")
        
        if (not index.get(column_value)):
            index[column_value] = {}

        index[column_value][rid] = True
    
    """
    # Takes a record and insert it into value_mapper, primary and secondary index
    """
    def insert_in_all_indices(self, columns):

        #deserialize
        
        primary_key = columns[self.key + NUM_HIDDEN_COLUMNS]
        if self.indices[self.key].get(primary_key):
            raise ValueError(f"Column with key: {columns[NUM_HIDDEN_COLUMNS + self.key]} already exists.")
        
        # insert in primary index 
        self.insert_to_index(self.key, primary_key, columns[RID_COLUMN])

        # map to value mapper
        self.value_mapper[primary_key] = columns[NUM_HIDDEN_COLUMNS:]

        #if we have secondary index, insert column value into secondary index
        for indexed_columns, index in self.secondary_index.items():

            key_column_number, value_column_number = indexed_columns
            key = columns[key_column_number + NUM_HIDDEN_COLUMNS]
            value = columns[value_column_number + NUM_HIDDEN_COLUMNS]

            # if secondary index key already have a value, add new value to the list
            if index.get(key):
                index[key].append(value)
            else:
                index[key] = [value]

        #serialize
        return True


    """
    # Remove element associated with primary key from value_mapper, primary and secondary index
    """
    def delete_from_all_indices(self, primary_key):

        #deserialize

        if not self.indices[self.key].get(primary_key):
            raise ValueError(f"Column with key: {primary_key} does not exist.")
        
        #delete from primary index 
        self.delete_from_index(self.key, primary_key)

        # if we have secondary index, delete from those too
        for indexed_columns, index in self.secondary_index.items():

            key_column_number, value_column_number = indexed_columns
            key = self.value_mapper[primary_key][key_column_number]
            value = self.value_mapper[primary_key][value_column_number]

            index[key].remove(value)

            if index[key] == []:
                del index[key]

        #delete from value mapper
        deleted_column = self.value_mapper[primary_key]
        del self.value_mapper[primary_key]

        #serialize

        return deleted_column
    
    """
    # Update to value_mapper and secondary index
    """
    def update_all_indices(self, primary_key, columns):

        #deserialize

        #get rid from primary key
        if not self.indices[self.key].get(primary_key):
            raise ValueError(f"Column with key: {primary_key} does not exist.")
        
        #update new columns in value_mapper
        for i in range(0, self.num_columns):
            if columns[NUM_HIDDEN_COLUMNS + i] != None:
            
                #if we have secondary index, update those
                for indexed_columns, index in self.secondary_index.items():
                    
                    #get previous column values
                    key_column_number, value_column_number = indexed_columns
                    key = self.value_mapper[primary_key][key_column_number]
                    value = self.value_mapper[primary_key][value_column_number]

                    #if changed value is in key column, transfer to new mapping to new key and delete old key
                    if key_column_number == i:
                        if index.get(columns[i + NUM_HIDDEN_COLUMNS]):

                            for value in index[key]:
                                index[columns[i + NUM_HIDDEN_COLUMNS]].append(value)

                        else:
                            index[columns[i + NUM_HIDDEN_COLUMNS]] = index[key]
                        del index[key]
                    
                    #if changed value is in value column, discard old value and add new value
                    if value_column_number == i:
                        index[key].remove(value)
                        index[key].append(columns[i + NUM_HIDDEN_COLUMNS])

                self.value_mapper[primary_key][i] = columns[i + NUM_HIDDEN_COLUMNS]

        #serialize

        return True
    
    # need to figure out path (?)
    """
    referencing from Raiyan's bufferpool directory in disk

    DB Directory: Folder
       -> TableName: Folder
          -> Index: Folder
            -> Primary_Index
            -> Value_Mapper    folder? or files?
            -> Secondary_Index: Folder   if we decide to create any
               -> Index_{key_column_number}_{value_column_number}      
    """
    def serialize(self, path, data):
        with open(path, 'wb') as file:
            pickle.dump(data, file)


    def deserialize(self, path):
        with open(path, 'rb') as file:
            return pickle.load(file)