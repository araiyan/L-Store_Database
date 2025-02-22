"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from lstore.config import *
from BTrees.OOBTree import OOBTree
import pickle
import os

class Index:

    def __init__(self, table):

        self.indices = [None] * table.num_columns

        self.num_columns = table.num_columns
        self.key = table.key
        #self.bufferpool = table.bufferpool
        #self.page_ranges = table.page_ranges
        self.table = table
        self.secondary_index = {}

        self.index_directory = os.path.join(table.table_path, "Index")
        self.path_to_value_mapper = os.path.join(self.index_directory, "Value_Mapper.pkl")

        if not os.path.exists(self.index_directory):
            os.makedirs(self.index_directory)

        #primary index and value mapper are recreated upon reopening db
        self.__generate_primary_index()

        if os.path.exists(self.path_to_value_mapper) and os.path.getsize(self.path_to_value_mapper) != 0:
            self.deserialize_value_mapper()
        else: 
            """
            #maps primary key to its most updated column values
            #self.value_mapper[primary_key] = [column1_value, column2_value...]
            """
            self.value_mapper = OOBTree()


    """
    # returns the location of all records with the given value on column "column" as a list
    # returns None if no rid found
    """
    def locate(self, column, value):

        if column != self.key:
            primary_keys = self.get(column, self.key, value)
            return self.__get_rid_from_subset_of_primary_key(primary_keys)
            
        return list(self.indices[column].get(value, [])) or None

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end" as a list
    # returns None if no rid found
    """

    def locate_range(self, begin, end, column):

        if column != self.key:

            primary_keys = self.get_range(column, self.key, begin, end)
            return self.__get_rid_from_subset_of_primary_key(primary_keys)

        return [rid for sublist in self.indices[self.key].values(min=begin, max=end) for rid in sublist.keys()] or None
    

    def __get_rid_from_subset_of_primary_key(self, primary_keys):
        all_rids = []
        for key in primary_keys:
                if key in self.indices[self.key]:
                    if self.indices[self.key][key].keys():
                        all_rids.append(list(self.indices[self.key][key].keys())[0])

        return all_rids if all_rids else None


    """
    # optional: Create index on specific columns 
    # index will have key and column mapped as {key_column_value1: [value_column_value1]}
    # or {key_column_value1: [value_column_value1, value_column_value2...]} if more than one value maps to the same key
    """
    def create_index(self, key_column_number, value_column_number):
        #deserialize value_mapper if lost

        if (key_column_number or value_column_number) >= self.table.num_columns:
            return False

        if (key_column_number, value_column_number) not in self.secondary_index:

            btree = OOBTree()

            # go through value mapper to create new index
            for _, column_values in self.value_mapper.items():

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
            return self.secondary_index[(key_column_number, value_column_number)]
        else:
            return False

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, key_column_number, value_column_number):

        if (key_column_number or value_column_number) >= self.table.num_columns:
            return False

        # clears Btree and removes reference 
        if self.secondary_index[(key_column_number, value_column_number)] != None:

            self.secondary_index[(key_column_number, value_column_number)].clear()
            del self.secondary_index[(key_column_number, value_column_number)]

        # if the index trying to drop doesn't exist in the dict of secondary indices
        else:
            return False

    
    def delete_from_index(self, column_index, column_value):
        '''Used to delete a single value from an index'''
        index:OOBTree = self.indices[column_index]

        if (index is None):
            return False
        
        if (index.get(column_value)):
            del index[column_value]
        else:
            return False

    def insert_to_index(self, column_index, column_value, rid):
        '''Used to insert the primary key for primary key indexing'''
        index:OOBTree = self.indices[column_index]

        if (index is None):
            return False
        
        if (not index.get(column_value)):
            index[column_value] = {}

        index[column_value][rid] = True
    
    """
    # Takes a record and insert it into value_mapper, primary and secondary index
    """
    def insert_in_all_indices(self, columns):
        
        primary_key = columns[self.key + NUM_HIDDEN_COLUMNS]
        if self.indices[self.key].get(primary_key):
            return False
        
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

        return True

    """
    # Remove element associated with primary key from value_mapper, primary and secondary index
    """
    def delete_from_all_indices(self, primary_key):

        if not self.indices[self.key].get(primary_key):
            return False
        
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

        return deleted_column
    
    """
    # Update to value_mapper and secondary index
    """
    def update_all_indices(self, primary_key, columns):

        #get rid from primary key
        if not self.indices[self.key].get(primary_key):
            return False
        
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

        return True
    

    """
    # get all rid from primary index for merge
    """
    def grab_all(self):
        all_rid = []
        for _, rids in self.indices[self.key].items():
            for rid in rids:
                all_rid.append(rid)
        return all_rid
    
    
    """
    # Use the methods below for accessing secondary index
    # decide if you want to create one first using index.create_index(key_column, value_column)
    # or the value will be searched through value mapper
    """

    """
    # basically the same as locate and locate range except it returns value in designated column 
    # instead of rid when given primary/secondary key and its column number
    """
    def get(self, key_column_number, value_column_number, key):

        if (key_column_number, value_column_number) in self.secondary_index:
            #search secondary index
            values = list(self.secondary_index[key_column_number, value_column_number][key])
            return values if values else None
        
        else:
            self.__search_value_mapper(key_column_number, value_column_number, key, key)


    def get_range(self, key_column_number, value_column_number, begin, end):

        if (key_column_number, value_column_number) in self.secondary_index:
            #search secondary index
            values = list(self.secondary_index[key_column_number, value_column_number].values(min=begin, max=end))
            return [item for sublist in values for item in sublist] or None

        else:
            self.__search_value_mapper(key_column_number, value_column_number, begin, end)


    def __search_value_mapper(self, key_column_number, value_column_number, begin, end):
        values = []
        for _, column_values in self.value_mapper.items():

                # get key and values from value mapper
            value_mapper_key = column_values[key_column_number]
            value_mapper_value = column_values[value_column_number]

            if begin <= value_mapper_key <= end:
                values.append(value_mapper_value)
            
        return values if values else None

    """
    referencing from Raiyan's bufferpool directory in disk

    DB Directory: Folder
       -> TableName: Folder
          -> Index: Folder
            -> Value_Mapper
    """
        
    # use this when open db
    def __generate_primary_index(self):
        #initialize a BTree
        self.indices[self.key] = OOBTree()

        #get base rid for every record with no duplicates
        all_base_rids = set(self.table.page_directory.values())

        #read through bufferpool to get primary index
        for rid in all_base_rids:
            page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)
            primary_key = self.table.bufferpool.read_page_slot(page_range_index, self.key, page_index, page_slot)

            #insert {primary_index: {rid: True}} into primary index BTree
            self.insert_to_index(self.key, primary_key, rid)
    
    # use this when open db
    def deserialize_value_mapper(self):
        with open(self.path_to_value_mapper, 'rb') as file:
            self.value_mapper = pickle.load(file)


    #use this when close db
    def serialize_value_mapper(self):
        with open(self.path_to_value_mapper, 'wb') as file:
            pickle.dump(self.value_mapper, file)