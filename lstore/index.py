"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
import re
from lstore.config import *
from BTrees.OOBTree import OOBTree
import pickle
import os

class Index:

    def __init__(self, table):

        self.indices = [None] * table.num_columns

        """
        #maps primary key to its most updated column values
        #self.value_mapper[primary_key] = [column1_value, column2_value...]
        """
        self.value_mapper = OOBTree()

        self.num_columns = table.num_columns
        self.key = table.key
        #self.bufferpool = table.bufferpool
        #self.page_ranges = table.page_ranges
        self.table = table

        self.create_index(self.key)

        self.index_directory = os.path.join(table.table_path, "Index")
        self.path_to_value_mapper = os.path.join(self.index_directory, "Value_Mapper.pkl")

        if not os.path.exists(self.index_directory):
            os.makedirs(self.index_directory)

        #primary index and value mapper are recreated upon reopening db

        self.deserialize_all_indices()

        self.__generate_primary_index()




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

        return [rid for sublist in self.indices[column].values(min=begin, max=end) for rid in sublist.keys()] or None


    """
    # optional: Create index on specific columns 
    # index will have key and column mapped as {key_column_value1: [value_column_value1]}
    # or {key_column_value1: [value_column_value1, value_column_value2...]} if more than one value maps to the same key
    """
    def create_index(self, column_number):
        #deserialize value_mapper if lost

        if column_number >= self.table.num_columns:
            return False

        if self.indices[column_number] == None:

            self.indices[column_number] = OOBTree()

            # go through value mapper to create new index
            for primary_key, column_values in self.value_mapper.items():

                # get value from value mapper and rid from primary index
                column_value = column_values[column_number]
                rid = list(self.indices[self.key][primary_key].keys())[0]

                self.insert_to_index(column_number, column_value, rid)

            return True
        else:
            return False

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):

        if column_number >= self.table.num_columns:
            return False

        # clears Btree and removes reference 
        if self.indices[column_number] != None:

            self.indices[column_number].clear()
            self.indices[column_number] = None

            #delete file 
            path = os.path.join(self.index_directory, f"index_{column_number}.pkl")
            if os.path.isfile(path):
                os.remove(path)

            return True
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
        #self.insert_to_index(self.key, primary_key, columns[RID_COLUMN])

        # map to value mapper
        self.value_mapper[primary_key] = columns[NUM_HIDDEN_COLUMNS:]

        #if we have secondary index, insert column value into secondary index
        for i in range(self.num_columns):

            if self.indices[i] != None:
                column_value = columns[i + NUM_HIDDEN_COLUMNS]
                rid = columns[RID_COLUMN]
                self.insert_to_index(i,column_value, rid)

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
        for i in range(self.num_columns):

            if self.indices[i] != None:

                key = self.value_mapper[primary_key][i]
                rid = self.indices[self.key][primary_key].keys()[0]

                del self.indices[i][key][rid]

                if self.indices[i][key] == {}:
                    
                    del self.indices[i][key]

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
            if columns[NUM_HIDDEN_COLUMNS + i] != None and self.indices[i] != None:
            

                    key = self.value_mapper[primary_key][i]
                    rid = self.indices[self.key][primary_key].keys()[0]

                    #if changed value is in key column, transfer to new mapping to new key and delete old key
                    if self.indices[i].get(columns[i + NUM_HIDDEN_COLUMNS]):

                        self.insert_to_index(i, columns[i + NUM_HIDDEN_COLUMNS, rid])

                    else:
                        self.indices[i][columns[i + NUM_HIDDEN_COLUMNS]] = self.indices[i][key]
                        self.insert_to_index(i, columns[i + NUM_HIDDEN_COLUMNS, rid])
                        del self.indices[i][key]
        

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
        self.__search_value_mapper(key_column_number, value_column_number, key, key)


    def get_range(self, key_column_number, value_column_number, begin, end):
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
            -> Value_Mapper.pkl
            -> index_{column_number}.pkl
    """
        
    # use this when open db
    def __generate_primary_index(self):
        #initialize a BTree
        self.indices[self.key] = OOBTree()

        #get base rid for every record with no duplicates
        all_base_rids = self.table.grab_all_base_rid()

        #read through bufferpool to get primary index
        for rid in all_base_rids:
            page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)
            primary_key = self.table.bufferpool.read_page_slot(page_range_index, self.key, page_index, page_slot)

            #insert {primary_index: {rid: True}} into primary index BTree
            self.insert_to_index(self.key, primary_key, rid)
    
    # use this when open db
    def deserialize(self, path, column_number):
        with open(path, 'rb') as file:
            self.indices[column_number] = pickle.load(file)


    #use this when close db
    def serialize(self, path, data):
        with open(path, 'wb') as file:
            pickle.dump(data, file)


    def serialize_all_indices(self):

        if not os.path.exists(self.index_directory):
            os.makedirs(self.index_directory)

        for i in range(self.num_columns):
            if self.indices[i] != None:
                path = os.path.join(self.index_directory, f"index_{i}.pkl")
                self.serialize(path, self.indices[i])
        
        self.serialize(self.path_to_value_mapper, self.value_mapper)
    

    def deserialize_all_indices(self):

        if os.path.exists(self.index_directory):

            index_file = r'index_(\d+)\.pkl'
            all_saved_index = os.listdir(self.index_directory)

            for index in all_saved_index:
                path = os.path.join(self.index_directory, index)
                is_index = re.match(index_file, index)
                if is_index:
                    column_number = int(is_index.group(1))

                    if os.path.exists(path) and os.path.getsize(path) != 0:
                        self.deserialize(path, column_number)
                else:
                    if os.path.exists(self.path_to_value_mapper) and os.path.getsize(self.path_to_value_mapper) != 0:
                        with open(path, 'rb') as file:
                            self.value_mapper = pickle.load(file)
