from lstore.index import Index
from lstore.page_range import PageRange, MergeRequest
from time import time
from lstore.config import *
from lstore.bufferpool import BufferPool
import json
import os
import threading
import queue
from typing import List

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        '''Each record contains both the hidden columns and the given columns [...HIDDEN_COLUMNS, ...GIVEN_COLUMNS]'''

    def __str__(self):
        return f"RID: {self.rid} Key: {self.key} \nColumns: {self.columns}"

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table(primary) key in column
    :db_path: string            #Path to the database directory where the table's data will be stored.
    """
    def __init__(self, name, num_columns, key, db_path):
        if (key < 0 or key >= num_columns):
            raise ValueError("Error Creating Table! Primary Key must be within the columns of the table")

        self.name = name
        self.key = key
        self.db_path = db_path
        self.table_path = os.path.join(db_path, name)
        self.num_columns = num_columns
        self.total_num_columns = num_columns + NUM_HIDDEN_COLUMNS

        self.page_directory = {}
        '''
        Page direcotry will map rids to consolidated rids
        page_directory[rid] = consolidated_rid
        consolidated_rid is treated the same as a base_rid
        Table_merge should be the only function that modifies the page_directory
        All others can access the page_dierctory
        '''
        self.page_directory_lock = threading.Lock()


        # initialize bufferpool in table, not DB
        self.bufferpool = BufferPool(self.table_path)
        self.page_ranges:List[PageRange] = []

        # setup queues for base rid allocation/deallocation
        self.deallocation_base_rid_queue = queue.Queue()
        self.allocation_base_rid_queue = queue.Queue()

        self.merge_queue = queue.Queue()
        '''stores the base rid of a record to be merged'''

        # The table should handle assigning RIDs
        self.rid_index = 0
        
        self.index = Index(self)
        # Start the merge thread
        # Note: This thread will stop running when the main program terminates
        self.merge_thread = threading.Thread(target=self.__merge, daemon=True)
        # self.merge_thread.start()

        # start the deallocation thread
        self.deallocation_thread = threading.Thread(target=self.__delete_worker, daemon=True)
        self.deallocation_thread.start()

    def assign_rid_to_record(self, record: Record):
        '''Use this function to assign a record's RID'''
        with self.page_directory_lock:
            
            # recycle unused RIDs
            if not self.allocation_base_rid_queue.empty():
                record.rid = self.allocation_base_rid_queue.get()
            else:
                record.rid = self.rid_index
                self.rid_index += 1

    def get_base_record_location(self, rid) -> tuple[int, int, int]:
        '''Returns the location of a record within base pages given a rid'''
        page_range_index = rid // (MAX_RECORD_PER_PAGE_RANGE)
        page_index = rid % (MAX_RECORD_PER_PAGE_RANGE) // MAX_RECORD_PER_PAGE
        page_slot = rid % MAX_RECORD_PER_PAGE
        return (page_range_index, page_index, page_slot)

    def insert_record(self, record: Record):
        page_range_index, page_index, page_slot = self.get_base_record_location(record.rid)

        if (page_range_index >= len(self.page_ranges)):
            self.page_ranges.append(PageRange(page_range_index, self.num_columns, self.bufferpool, self.merge_queue))
        
        current_page_range:PageRange = self.page_ranges[page_range_index]

        current_page_range.write_base_record(page_index, page_slot, record.columns)   
    
    def __merge(self):
        print("Merge is happening")

        while True:
            # Block ensures that we wait for a record to be added to the queue first
            # before we continue merging a record
            merge_request:MergeRequest = self.merge_queue.get(block=True)

            # make a copy of the base page for the recieved rid
            start_rid = merge_request.page_range_index * MAX_RECORD_PER_PAGE_RANGE
            end_rid = min(start_rid + MAX_RECORD_PER_PAGE_RANGE, self.rid_index)

            current_page_range:PageRange = self.page_ranges[merge_request.page_range_index]

            for rid in range(start_rid, end_rid):
                _, page_index, page_slot = self.get_base_record_location(rid)

                base_record_columns = current_page_range.copy_base_record(page_index, page_slot)
                base_merge_time = base_record_columns[UPDATE_TIMESTAMP_COLUMN]

                if (base_merge_time is None):
                    base_merge_time = 0
                    if (self.__insert_base_copy_to_tail_pages(current_page_range, base_record_columns) is False):
                        continue

                # Get the latest record
                current_rid = base_record_columns[INDIRECTION_COLUMN]
                latest_schema_encoding = base_record_columns[SCHEMA_ENCODING_COLUMN]
                current_time_stamp = current_page_range.read_tail_record_column(current_rid, TIMESTAMP_COLUMN)

                # if current rid < MAX_RECORD_PER_PAGE_RANGE, then we are at the base record
                while current_rid >= MAX_RECORD_PER_PAGE_RANGE and latest_schema_encoding != 0 and current_time_stamp > base_merge_time:
                    indirection_column = current_page_range.read_tail_record_column(current_rid, INDIRECTION_COLUMN)
                    schema_encoding = current_page_range.read_tail_record_column(current_rid, SCHEMA_ENCODING_COLUMN)
                    current_time_stamp = current_page_range.read_tail_record_column(current_rid, TIMESTAMP_COLUMN)
                    
                    for col_index in range(self.num_columns):
                        if (latest_schema_encoding & (1 << col_index)) and (schema_encoding & (1 << col_index)):
                            latest_schema_encoding ^= (1 << col_index)
                            base_record_columns[col_index + NUM_HIDDEN_COLUMNS] = current_page_range.read_tail_record_column(current_rid, col_index + NUM_HIDDEN_COLUMNS)

                    current_rid = indirection_column

                base_record_columns[UPDATE_TIMESTAMP_COLUMN] = int(time())
                self.bufferpool.write_page_slot(merge_request.page_range_index, UPDATE_TIMESTAMP_COLUMN, page_index, page_slot, base_record_columns[UPDATE_TIMESTAMP_COLUMN])
            

            self.merge_queue.task_done()

    def __insert_base_copy_to_tail_pages(self, page_range:PageRange, base_record_columns):
        '''Inserts a copy of the base record to the last tail page of the record'''
        logical_rid = page_range.assign_logical_rid()
        indirection_rid = base_record_columns[INDIRECTION_COLUMN]
        last_indirection_rid = page_range.find_records_last_logical_rid(indirection_rid)

        # if no tail record exist return false
        if (last_indirection_rid == indirection_rid):
            return False
        
        page_index, page_slot = page_range.get_column_location(last_indirection_rid, INDIRECTION_COLUMN)
        page_range.write_tail_record(logical_rid, *base_record_columns)

        # edit the last page's indirection column to point to the new copied base record
        self.bufferpool.write_page_slot(page_range.page_range_index, INDIRECTION_COLUMN, page_index, page_slot, logical_rid)

        return True
    
    def grab_all_base_rids(self):
        '''Returns a list of all base rids'''
        return list(range(self.rid_index))
        
    def serialize(self):
        """Returns table metadata as a JSON-compatible dictionary"""
        return {
            "table_name": self.name,
            "num_columns": self.num_columns,
            "key_index": self.key,
            "page_directory": self.page_directory,
            "rid_index": self.rid_index
        }

    def __delete_worker(self):
        '''
        1. Grabs a RID from `deallocation_base_rid_queue`. 
        2. Moves the base RID to `allocation_base_rid_queue` for reuse.
        3. Traverses all tail records and moves their logical RIDs to `allocation_logical_rid_queue` in the corresponding PageRange
        '''
        while True:
            
            # process base rid deletions (retrieve rid from base deallocation queue)
            rid = self.deallocation_base_rid_queue.get(block=True)

            # locate page range given rid
            page_range_idx, page_idx, page_slot = self.get_base_record_location(rid)
            page_range = self.page_ranges[page_range_idx]

            self.allocation_base_rid_queue.put(rid)

            logical_rid = page_range.bufferpool.read_page_slot(page_range_idx, INDIRECTION_COLUMN, page_idx, page_slot)   

            # traverse 
            while logical_rid >= MAX_RECORD_PER_PAGE_RANGE:
                page_range.allocation_logical_rid_queue.put(logical_rid)
                logical_page_index, logical_page_slot = page_range.get_column_location(logical_rid, INDIRECTION_COLUMN)
                logical_rid = page_range.bufferpool.read_page_slot(page_range_idx, INDIRECTION_COLUMN, logical_page_index, logical_page_slot)
        
            self.deallocation_base_rid_queue.task_done()