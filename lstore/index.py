"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
import base64
from lstore.config import *
from BTrees.OOBTree import OOBTree
import pickle
import threading

class Index:

    def __init__(self, table):

        self.indices = [None] * table.num_columns

        self.num_columns = table.num_columns
        self.key = table.key
        self.table = table

        has_stored_index  = self.__generate_primary_index()

        if not has_stored_index:
            self.create_index(self.key)

        self.index_lock = threading.Lock()

    """
    # returns the location of all records with the given value on column "column" as a list
    # returns None if no rid found
    """
    def locate(self, column, value):
        with self.index_lock:
            if self.indices[column] == None:
                return False
            
            return list(self.indices[column].get(value, [])) or None
    
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end" as a list
    # returns None if no rid found
    """

    def locate_range(self, begin, end, column):
        with self.index_lock:
            if self.indices[column] == None:
                return False

            return [rid for sublist in self.indices[column].values(min=begin, max=end) for rid in sublist.keys()] or None


    # create secondary index through page range and bufferpool    
    def create_index(self, column_number):

        if column_number >= self.table.num_columns:
            return False

        if self.indices[column_number] == None:

            self.indices[column_number] = OOBTree()
            # go through value mapper to create new index
            all_base_rids = self.grab_all()

            column_value =  None
            tail_timestamp = 0
            tail = False
            #read through bufferpool to get latest tail record
            for rid in all_base_rids:

                page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)

                indir_rid = self.table.bufferpool.read_page_slot(page_range_index, INDIRECTION_COLUMN, page_index, page_slot)
                frame_num  = self.table.bufferpool.get_page_frame_num(page_range_index, INDIRECTION_COLUMN, page_index)
                self.table.bufferpool.mark_frame_used(frame_num)

                base_schema = self.table.bufferpool.read_page_slot(page_range_index, SCHEMA_ENCODING_COLUMN, page_index, page_slot)
                frame_num  = self.table.bufferpool.get_page_frame_num(page_range_index, SCHEMA_ENCODING_COLUMN, page_index)
                self.table.bufferpool.mark_frame_used(frame_num)

                """ Referencing latest tail page search from sum version """
                if indir_rid == (rid % MAX_RECORD_PER_PAGE_RANGE) : # if no updates

                    column_value = self.table.bufferpool.read_page_slot(page_range_index, column_number + NUM_HIDDEN_COLUMNS, page_index, page_slot)
                    frame_num  = self.table.bufferpool.get_page_frame_num(page_range_index, column_number + NUM_HIDDEN_COLUMNS, page_index)
                    self.table.bufferpool.mark_frame_used(frame_num)

                else: #if updates

                    base_timestamp = self.table.bufferpool.read_page_slot(page_range_index, TIMESTAMP_COLUMN, page_index, page_slot)
                    frame_num  = self.table.bufferpool.get_page_frame_num(page_range_index, TIMESTAMP_COLUMN, page_index)
                    self.table.bufferpool.mark_frame_used(frame_num)

                    if(base_schema >> column_number) & 1:

                        while True:
                            try:
                                tail_page_index, tail_slot = self.table.page_ranges[page_range_index].get_column_location(indir_rid, column_number + NUM_HIDDEN_COLUMNS)
                                tail_timestamp = self.table.page_ranges[page_range_index].read_tail_record_column(indir_rid, TIMESTAMP_COLUMN)
                                tail = True
                                break
                            except:

                                prev_rid = indir_rid
                                indir_rid = self.table.page_ranges[page_range_index].read_tail_record_column(indir_rid, INDIRECTION_COLUMN)

                                if indir_rid == rid: #edge case where latest updated column value is in the first tail record inserted
                                    least_updated_tail_rid = self.table.page_ranges[page_range_index].read_tail_record_column(prev_rid, RID_COLUMN)
                                    tail_page_index, tail_slot = self.table.page_ranges[page_range_index].get_column_location(least_updated_tail_rid, column_number + NUM_HIDDEN_COLUMNS)
                                    tail_timestamp = self.table.page_ranges[page_range_index].read_tail_record_column(least_updated_tail_rid, TIMESTAMP_COLUMN)
                                    tail = True
                                    break

                    # if the tail page for column is latest updated 
                    if (base_schema >> column_number) & 1 and tail_timestamp >= base_timestamp and tail:
                        column_value = self.table.bufferpool.read_page_slot(page_range_index, column_number + NUM_HIDDEN_COLUMNS, tail_page_index, tail_slot)
                        frame_num  = self.table.bufferpool.get_page_frame_num(page_range_index, column_number + NUM_HIDDEN_COLUMNS, page_index)
                        self.table.bufferpool.mark_frame_used(frame_num)

                    else: # if merged page is latest updated
                        column_value = self.table.bufferpool.read_page_slot(page_range_index, column_number + NUM_HIDDEN_COLUMNS, page_index, page_slot)
                        frame_num  = self.table.bufferpool.get_page_frame_num(page_range_index, column_number + NUM_HIDDEN_COLUMNS, page_index)
                        self.table.bufferpool.mark_frame_used(frame_num)
                
                #insert {primary_index: {rid: True}} into primary index BTree
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

        return True
    
    """
    # Takes a record and insert it into value_mapper, primary and secondary index
    """
    def insert_in_all_indices(self, columns):
        
        primary_key = columns[self.key + NUM_HIDDEN_COLUMNS]
        if self.indices[self.key].get(primary_key):
            return False

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
    def delete_from_all_indices(self, primary_key, prev_columns):
        with self.index_lock:
            if not self.indices[self.key].get(primary_key):
                return False
            
            rid = list(self.indices[self.key][primary_key].keys())[0]

            # if we have secondary index, delete from those too
            for i in range(self.num_columns):

                if (self.indices[i] != None) and (prev_columns[i] != None):

                    del self.indices[i][prev_columns[i]][rid]

                    if self.indices[i][prev_columns[i]] == {}:
                        del self.indices[i][prev_columns[i]]

            return True
    
    """
    # Update to value_mapper and secondary index
    """
    def update_all_indices(self, primary_key, new_columns, prev_columns):
        with self.index_lock:
            #get rid from primary key
            if not self.indices[self.key].get(primary_key):
                return False
            
            #update primary key first if needed
            if (new_columns[NUM_HIDDEN_COLUMNS + self.key] != None) and  (self.indices[self.key] != None) and (prev_columns[self.key] != None):

                rid = list(self.indices[self.key][primary_key].keys())[0]

                self.insert_to_index(self.key, new_columns[self.key + NUM_HIDDEN_COLUMNS], rid)
                del self.indices[self.key][primary_key]
                            
                primary_key = new_columns[self.key + NUM_HIDDEN_COLUMNS]
            
            #update other indices
            for i in range(0, self.num_columns):
                if (new_columns[NUM_HIDDEN_COLUMNS + i] != None) and (self.indices[i] != None) and (prev_columns[i] != None) and (i != self.key):
                
                    key = prev_columns[i]
                    rid = list(self.indices[self.key][primary_key].keys())[0]

                    #if changed value is in key column, transfer to new mapping to new key and delete old key
                    if self.indices[i].get(new_columns[i + NUM_HIDDEN_COLUMNS]):
                        self.insert_to_index(i, new_columns[i + NUM_HIDDEN_COLUMNS], rid)
                        del self.indices[i][key][rid]

                    else:
                        self.insert_to_index(i, new_columns[i + NUM_HIDDEN_COLUMNS], rid)
                        del self.indices[i][key][rid]
                        if self.indices[i][key] == {}:
                    
                            del self.indices[i][key]

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
        
    # use this when open db
    def __generate_primary_index(self):

        #get base rid for every record with no duplicates
        all_base_rids = self.table.grab_all_base_rids()
        if not all_base_rids:
            return False
        
        self.indices[self.key] = OOBTree()
        frames_used = []
        
        #read through bufferpool to get primary index
        for rid in all_base_rids:
            page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)
            primary_key = self.table.bufferpool.read_page_slot(page_range_index, self.key + NUM_HIDDEN_COLUMNS, page_index, page_slot)
            frame_num  = self.table.bufferpool.get_page_frame_num(page_range_index, self.key + NUM_HIDDEN_COLUMNS, page_index)
            frames_used.append(frame_num)
            #insert {primary_index: {rid: True}} into primary index BTree
            self.insert_to_index(self.key, primary_key, rid)
        
        for frame in frames_used:
            self.table.bufferpool.mark_frame_used(frame)

        return True
    
    # call "index:": self.index.serialize() from table
    # pickle every index BTree and then store them in base64
    def serialize(self):

        all_serialized_data = {}

        for i in range(self.num_columns):
            if self.indices[i] != None:
                pickled_index = pickle.dumps(self.indices[i])
                encoded_pickled_index = base64.b64encode(pickled_index).decode('utf-8')
                all_serialized_data[f"index[{i}]"] = encoded_pickled_index

        return all_serialized_data
    
    # call index.deserialize(json_data["Index"]) from table
    def deserialize(self, data):

        for i in range(self.num_columns):
            index_column_number = f"index[{i}]"
            if index_column_number in data:
                decoded_index = base64.b64decode(data[index_column_number])
                self.indices[i] = pickle.loads(decoded_index)


    def exist_index(self, column_number):
        if self.indices[column_number] != None:
            return True
        else:
            return False
        