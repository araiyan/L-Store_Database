"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
import base64
import threading
from lstore.config import *
from BTrees.OOBTree import OOBTree
import pickle

from lstore.lock import LockManager

class Index:

    def __init__(self, table):

        self.indices = [None] * table.num_columns

        self.num_columns = table.num_columns
        self.key = table.key
        self.table = table

        self.lock_manager = table.lock_manager

        # self.transaction_locks = {transaction1: {index_id1: locktype}}
        self.transaction_locks = {}
        self.record_transaction_lock = threading.Lock()
        self.serialize_lock = threading.Lock()

        self.indices[self.key] = OOBTree()


    """
    # returns the location of all records with the given value on column "column" as a list
    # returns None if no rid found
    """
    # keep tracks of locks used and only release at the end

    def locate(self, column, value):
        transaction_id = threading.get_ident()
        self.__lock_index(column, "S", transaction_id)

        if self.indices[column] is None:
            return False
            
        rid = list(self.indices[column].get(value, [])) or None
        #self.lock_manager.release_all_locks(transaction_id)
        return rid

    
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end" as a list
    # returns None if no rid found
    """

    def locate_range(self, begin, end, column):

        transaction_id = threading.get_ident()
        self.__lock_index(column, "S", transaction_id)
        if self.indices[column] == None:
            return False
        
        rid = [rid for sublist in self.indices[column].values(min=begin, max=end) for rid in sublist.keys()] or None
        #self.lock_manager.release_all_locks(transaction_id)
        return rid


    # create secondary index through page range and bufferpool    
    def create_index(self, column_number):

        if column_number >= self.table.num_columns:
            return False

        if self.indices[column_number] == None:

            transaction_id = threading.get_ident()

            #lock the entire index that's getting created
            self.__lock_index(column_number, "X", transaction_id)

            self.indices[column_number] = OOBTree()
            # go through value mapper to create new index
            all_base_rids = self.grab_all()

            column_value =  None
            tail_timestamp = 0
            tail = False

            #read through bufferpool to get latest tail record
            for rid in all_base_rids:

                self.__check_and_lock(transaction_id, rid, "S")
                page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)
                self.__check_and_lock(transaction_id, page_index, "IS")

                base_schema = self.__lock_read_mark(page_range_index, page_index, page_slot, SCHEMA_ENCODING_COLUMN, transaction_id, rid)
                indir_rid = self.__lock_read_mark(page_range_index, page_index, page_slot, INDIRECTION_COLUMN, transaction_id, rid)

                self.__check_and_lock(transaction_id, indir_rid, "S")

                """ Referencing latest tail page search from sum version """
                if indir_rid == (rid % MAX_RECORD_PER_PAGE_RANGE) : # if no updates

                    column_value = self.__lock_read_mark(page_range_index, page_index, page_slot, column_number + NUM_HIDDEN_COLUMNS, transaction_id, rid)

                else: #if updates

                    base_timestamp = self.__lock_read_mark(page_range_index, page_index, page_slot, TIMESTAMP_COLUMN, transaction_id, rid)

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
                                    self.__check_and_lock(transaction_id, prev_rid, "S")

                                    least_updated_tail_rid = self.table.page_ranges[page_range_index].read_tail_record_column(prev_rid, RID_COLUMN)
                                    tail_page_index, tail_slot = self.table.page_ranges[page_range_index].get_column_location(least_updated_tail_rid, column_number + NUM_HIDDEN_COLUMNS)
                                    tail_timestamp = self.table.page_ranges[page_range_index].read_tail_record_column(least_updated_tail_rid, TIMESTAMP_COLUMN)
                                    tail = True
                                    break

                                self.__check_and_lock(transaction_id, indir_rid, "S")

                    # if the tail page for column is latest updated 
                    if (base_schema >> column_number) & 1 and tail_timestamp >= base_timestamp and tail:
                        column_value = self.__lock_read_mark(page_range_index, tail_page_index, tail_slot, column_number + NUM_HIDDEN_COLUMNS, transaction_id, indir_rid)

                    else: # if merged page is latest updated
                        column_value = self.__lock_read_mark(page_range_index, page_index, page_slot, column_number + NUM_HIDDEN_COLUMNS, transaction_id, rid)
                
                #insert {primary_index: {rid: True}} into primary index BTree
                self.insert_to_index(column_number, column_value, rid)

            #self.lock_manager.release_all_locks(transaction_id)
            return True
        else:

            #self.lock_manager.release_all_locks(transaction_id)
            return False


    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):

        transaction_id = threading.get_ident()

        if column_number >= self.table.num_columns:
            return False
        
        self.__lock_index(column_number, "X", transaction_id)
        # clears Btree and removes reference 
        if self.indices[column_number] != None:

            self.indices[column_number].clear()
            self.indices[column_number] = None

            #self.lock_manager.release_all_locks(transaction_id)
            return True
        else:
            #self.lock_manager.release_all_locks(transaction_id)
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

        transaction_id = threading.get_ident()
        self.__lock_index(self.key, "S", transaction_id)
        self.__lock_index(self.key, "IX", transaction_id)

        primary_key = columns[self.key + NUM_HIDDEN_COLUMNS]
        if self.indices[self.key].get(primary_key):
            return False

        #if we have secondary index, insert column value into secondary index
        for i in range(self.num_columns):

            if self.indices[i] != None:
                column_value = columns[i + NUM_HIDDEN_COLUMNS]
                rid = columns[RID_COLUMN]

                if i == self.key:
                    self.__lock_index_tuple(i, columns[i + NUM_HIDDEN_COLUMNS], "X", transaction_id)
                else:
                    self.__lock_index(i, "S", transaction_id)
                    self.__lock_index(i, "IX", transaction_id)
                    self.__lock_index_tuple(i, columns[i + NUM_HIDDEN_COLUMNS], "X", transaction_id)

                self.insert_to_index(i, column_value, rid)

        #self.lock_manager.release_all_locks(transaction_id)
        return True

    """
    # Remove element associated with primary key from value_mapper, primary and secondary index
    """
    def delete_from_all_indices(self, primary_key, prev_columns):

        transaction_id = threading.get_ident()
        self.__lock_index(self.key, "S", transaction_id)
        self.__lock_index(self.key, "IX", transaction_id)

        if not self.indices[self.key].get(primary_key):
            return False
        
        rid = list(self.indices[self.key][primary_key].keys())[0]

        for i in range(self.num_columns):

            if (self.indices[i] != None) and (prev_columns[i] != None):

                if i == self.key:
                    self.__lock_index_tuple(i, prev_columns[i], "X", transaction_id)
                else:
                    self.__lock_index(i, "S", transaction_id)
                    self.__lock_index(i, "IX", transaction_id)
                    self.__lock_index_tuple(i, prev_columns[i], "X", transaction_id)

                del self.indices[i][prev_columns[i]][rid]
                if self.indices[i][prev_columns[i]] == {}:
                    del self.indices[i][prev_columns[i]]

        #self.lock_manager.release_all_locks(transaction_id)
        return True
    
    """
    # Update to value_mapper and secondary index
    """
    def update_all_indices(self, primary_key, new_columns, prev_columns):

        transaction_id = threading.get_ident()
        self.__lock_index(self.key, "S", transaction_id)
        self.__lock_index(self.key, "IX", transaction_id)

        #get rid from primary key
        if not self.indices[self.key].get(primary_key):
            return False
        
        #update primary key first if needed
        if (new_columns[NUM_HIDDEN_COLUMNS + self.key] != None) and  (self.indices[self.key] != None) and (prev_columns[self.key] != None):

            rid = list(self.indices[self.key][primary_key].keys())[0]

            self.__lock_index_tuple(self.key, primary_key, "X", transaction_id)
            self.__lock_index_tuple(self.key, new_columns[self.key + NUM_HIDDEN_COLUMNS], "X", transaction_id)

            self.insert_to_index(self.key, new_columns[self.key + NUM_HIDDEN_COLUMNS], rid)
            del self.indices[self.key][primary_key]
                        
            primary_key = new_columns[self.key + NUM_HIDDEN_COLUMNS]
        
        #update other indices
        for i in range(0, self.num_columns):
            if (new_columns[NUM_HIDDEN_COLUMNS + i] != None) and (self.indices[i] != None) and (prev_columns[i] != None) and (i != self.key):
                    
                    self.__lock_index(i, "S", transaction_id)
                    self.__lock_index(i, "IX", transaction_id)
                    self.__lock_index_tuple(i, prev_columns[i], "X", transaction_id)
                    self.__lock_index_tuple(i, new_columns[i + NUM_HIDDEN_COLUMNS], "X", transaction_id)

                    key = prev_columns[i]
                    rid = list(self.indices[self.key][new_columns[self.key + NUM_HIDDEN_COLUMNS]].keys())[0]

                    #if changed value is in key column, transfer to new mapping to new key and delete old key
                    if self.indices[i].get(new_columns[i + NUM_HIDDEN_COLUMNS]):
                        self.insert_to_index(i, new_columns[i + NUM_HIDDEN_COLUMNS], rid)
                        del self.indices[i][key][rid]

                    else:
                        self.insert_to_index(i, new_columns[i + NUM_HIDDEN_COLUMNS], rid)
                        del self.indices[i][key][rid]
                        if self.indices[i][key] == {}:
                    
                            del self.indices[i][key]

        #self.lock_manager.release_all_locks(transaction_id)
        return True
    

    def grab_all(self):

        transaction_id = threading.get_ident()
        self.__lock_index(self.key, "S", transaction_id)

        all_rid = []
        for _, rids in self.indices[self.key].items():
            for rid in rids:
                all_rid.append(rid)

        return all_rid
        
    # use this when open db
    """
    def __generate_primary_index(self):

        #get base rid for every record with no duplicates
        all_base_rids = self.table.grab_all_base_rids()
        if not all_base_rids:
            return False
        
        transaction_id = threading.get_ident()
        self.__lock_index(self.key, "X", transaction_id)

        self.indices[self.key] = OOBTree()
        frames_used = []
        
        #read through bufferpool to get primary index
        for rid in all_base_rids:

            if not self.__contains_lock(transaction_id, rid, "S"):
                self.lock_manager.acquire_lock(transaction_id, rid, "S")
                self.__record_lock_acquiring(transaction_id, rid, "S")

            page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)

            if not self.__contains_lock(transaction_id, page_index, "IS"):
                self.lock_manager.acquire_lock(transaction_id, page_index, "IS")
                self.__record_lock_acquiring(transaction_id, page_index, "IS")

            primary_key = self.table.bufferpool.read_page_slot(page_range_index, self.key + NUM_HIDDEN_COLUMNS, page_index, page_slot)
            frame_num  = self.table.bufferpool.get_page_frame_num(page_range_index, self.key + NUM_HIDDEN_COLUMNS, page_index)
            frames_used.append(frame_num)
            #insert {primary_index: {rid: True}} into primary index BTree
            self.insert_to_index(self.key, primary_key, rid)
        
        for frame in frames_used:
            self.table.bufferpool.mark_frame_used(frame)

        return True
    """
    
    # call "index:": self.index.serialize() from table
    # pickle every index BTree and then store them in base64
    def serialize(self):
        with self.serialize_lock:
            all_serialized_data = {}

            for i in range(self.num_columns):
                if self.indices[i] != None:
                    pickled_index = pickle.dumps(self.indices[i])
                    encoded_pickled_index = base64.b64encode(pickled_index).decode('utf-8')
                    all_serialized_data[f"index[{i}]"] = encoded_pickled_index

            return all_serialized_data
    
    # call index.deserialize(json_data["Index"]) from table
    def deserialize(self, data):
        with self.serialize_lock:
            for i in range(self.num_columns):
                index_column_number = f"index[{i}]"
                if index_column_number in data:
                    decoded_index = base64.b64decode(data[index_column_number])
                    self.indices[i] = pickle.loads(decoded_index)


    def exist_index(self, column_number):

        transaction_id = threading.get_ident()
        self.__lock_index(column_number, "S", transaction_id)

        if self.indices[column_number] != None:
            return True
        else:
            return False
    

    def __lock_read_mark(self, page_range_index, page_index,page_slot, column_number, transaction_id, record_id):

        self.__check_and_lock(transaction_id, record_id, "S")

        item_read = self.table.bufferpool.read_page_slot(page_range_index, column_number, page_index, page_slot)
        frame_num  = self.table.bufferpool.get_page_frame_num(page_range_index, INDIRECTION_COLUMN, page_index)
        self.table.bufferpool.mark_frame_used(frame_num)

        return item_read

    def __lock_index(self, column_number, lock_type, transaction_id):
        index_id = f"index{column_number}"
        self.__check_and_lock(transaction_id, index_id, lock_type)

    def __update_lock_index(self, transaction_id, column_number, cur_lock_type, new_lock_type):
        index_id = f"index{column_number}"
        if not self.__contains_lock(transaction_id, index_id, new_lock_type):
            self.lock_manager.upgrade_lock(transaction_id, index_id, cur_lock_type, new_lock_type)
            self.__record_lock_acquiring(transaction_id, index_id, new_lock_type)

    def __lock_index_tuple(self, column_number, column_value, lock_type, transaction_id):
        index_tuple_id = f"index{column_number}_{column_value}"
        self.__check_and_lock(transaction_id, index_tuple_id, lock_type)
    
    def get_index_id(self, column_number):
        return f"index{column_number}"
    
    def __check_and_lock(self, transaction_id, record_id, lock_type):
        if not self.__contains_lock(transaction_id, record_id, lock_type):
            self.lock_manager.acquire_lock(transaction_id, record_id, lock_type)
            self.__record_lock_acquiring(transaction_id, record_id, lock_type)
    
    # self.transaction_locks = {transaction1: {index_id1: ["S", "IX", "IS"...]}}
    def __contains_lock(self, transaction_id, index_id, lock_type):
        if transaction_id in self.transaction_locks and index_id in self.transaction_locks[transaction_id] and lock_type in self.transaction_locks[transaction_id].get(index_id, []):
            return True
        else:
            return False
        
    def __record_lock_acquiring(self, transaction_id, index_id, lock_type):
        with self.record_transaction_lock:
            if transaction_id not in self.transaction_locks:
                self.transaction_locks[transaction_id] = {}

            if index_id not in self.transaction_locks[transaction_id]:
                self.transaction_locks[transaction_id][index_id] = []

            if lock_type not in self.transaction_locks[transaction_id][index_id]:
                self.transaction_locks[transaction_id][index_id].append(lock_type)
