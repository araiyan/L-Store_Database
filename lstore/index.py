"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from lstore.config import *
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
        #self.table = table
        #only need primary index
        #for i in range(table.num_columns):
        #    self.create_index(i)

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
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        
        # Initialize indices as a list of empty dictionaries for each column
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
        for key, all_rid in column.items():
            
            # if more than one rid are mapped to a value
            if len(all_rid) > 1:
                for value in all_rid:
                    if value == rid:
                        return key
            else:
                if all_rid == rid:
                    return key
        return None 

    """
    def search_value(self, primary_key, all_rid, column_number):
        for rid in all_rid:
            # check if given key matches any rid in the set of rid
            # if key matches, find the column value
            if isinstance(self.table.page_directory[rid], tuple):
                page_index, page_slot = self.table.page_directory[rid][0]
                if primary_key == self.table.base_pages[self.key + NUM_HIDDEN_COLUMNS][page_index].get(page_slot):
                    return self.table.base_pages[column_number][page_index].get(page_slot)
            else:
                page_index, page_slot = self.table.page_directory[rid][self.key + NUM_HIDDEN_COLUMNS]
                if primary_key == self.table.tail_pages[self.key + NUM_HIDDEN_COLUMNS][page_index].get(page_slot):
                    return self.table.tail_pages[column_number][page_index].get(page_slot)
        return None
    """
    
    def delete_from_index(self, column_index, column_value, rid):
        '''Used to delete a single value from an index'''
        index:OOBTree = self.indices[column_index]


        if (index is None):
            raise IndexError("No indicy in the specified column")
        

        if (index.get(column_value)):
            del index[column_value][rid]
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
                if i == self.key:
                    value = primary_key
                else:
                    #value = self.search_value(primary_key, temp_rid, i + NUM_HIDDEN_COLUMNS)
                    value = self.search_value(self.indices[i],temp_rid)
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
    def delete_from_all_indices(self, primary_key):
        rid = self.indices[self.key][primary_key]
        if rid == None:
            return False
        for i in range(0, self.num_columns):
            if self.indices[i] != None:
                # look for value matched to rid
                if i == self.key:
                    value = primary_key
                else:
                    value = self.search_value(primary_key, temp_rid, i + NUM_HIDDEN_COLUMNS)


    """
    # Remove element associated with rid : primary key from all indices
    """
    def delete_from_all_indices(self, primary_key):
        rid = self.indices[self.key][primary_key]
        if rid == None:
            return False
        for i in range(0, self.num_columns):
            if self.indices[i] != None:
                # look for value matched to rid
                if i == self.key:
                    value = primary_key
                else:
                    #value = self.search_value(primary_key, temp_rid, i + NUM_HIDDEN_COLUMNS)
                    value = self.search_value(self.indices[i],rid)
                    
                #discard rid from the set, if it became empty set, delete the value too                
                self.indices[i][value].discard(list(rid)[0])
                if self.indices[i][value] == set():
                    del self.indices[i][value]
        return True
    
    #added to test table serialize, basec testing is working fine
    def serialize(self):
        """Serializes the Index to a JSON-compatible dictionary"""
        print("start index des", self.indices)
        serialized_indices = []
        for index in self.indices:
            if index is None:
                serialized_indices.append(None)
            else:
                serialized_index = {}
                for key, rids in index.items():
                    serialized_index[key] = list(rids)  # Convert set of RIDs to list
                serialized_indices.append(serialized_index)

        print("emd index des")
        return {
            "indices": serialized_indices,
            "value_mapper": self.value_mapper,
            "num_columns": self.num_columns,
            "key": self.key
        }
        
    #added to test table serialize, basec testing is working fine    
    def deserialize(self, data):
        """Deserializes the Index from JSON data"""
        self.num_columns = int(data['num_columns'])
        self.key = int(data['key'])

        deserialized_indices = []
        for index_data in data['indices']:
            if index_data is None:
                deserialized_indices.append(None)
            else:
                index = OOBTree()
                # Convert keys back to integers
                for key, rids in index_data.items():
                    index[int(key)] = set(rids)  # Convert list of RIDs back to set
                deserialized_indices.append(index)
        self.indices = deserialized_indices

        # Convert keys in value_mapper back to integers
        deserialized_value_mapper = []
        for vm in data['value_mapper']:
            if vm is None:
                deserialized_value_mapper.append(None)
            else:
                deserialized_vm = {int(k): v for k, v in vm.items()}
                deserialized_value_mapper.append(deserialized_vm)
        self.value_mapper = deserialized_value_mapper