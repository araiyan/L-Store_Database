from lstore.index import Index
from lstore.page_range import PageRange
from time import time
from lstore.config import *
from lstore.bufferpool import BufferPool

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
    :param key: int             #Index of table key in columns
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
        self.index = Index(self)

        # initialize bufferpool in table, not DB
        self.bufferpool = BufferPool(self.table_path)
        self.page_ranges:List[PageRange] = []
    
        self.diallocation_rid_queue = queue.Queue()
        self.merge_queue = queue.Queue()
        '''stores the base rid of a record to be merged'''

        # The table should handle assigning RIDs
        self.rid_index = 0

    def assign_rid_to_record(self, record: Record):
        '''Use this function to assign a record's RID'''
        
        record.rid = self.rid_index
        self.rid_index += 1

    def get_base_record_location(self, rid) -> tuple[int, int, int]:
        '''Returns the location of a record within base pages given a rid'''
        page_range_index = rid // (MAX_PAGE_RANGE * MAX_RECORD_PER_PAGE)
        page_index = rid % (MAX_PAGE_RANGE * MAX_RECORD_PER_PAGE) // MAX_RECORD_PER_PAGE
        page_slot = rid % MAX_RECORD_PER_PAGE
        return (page_range_index, page_index, page_slot)

    def insert_record(self, record: Record):
        page_range_index, page_index, page_slot = self.get_base_record_location(record.rid)

        if (page_range_index >= len(self.page_ranges)):
            self.page_ranges.append(PageRange(page_range_index, self.num_columns, self.bufferpool))
        
        current_page_range:PageRange = self.page_ranges[page_range_index]

        current_page_range.write_base_record(page_index, page_slot, *record.columns)

    
    def grab_tail_value_from_page_location(self, tail_location, column):
        page_index, page_slot = tail_location[column]
        return self.tail_pages[column][page_index].get(page_slot)
        
    def __grab_tail_value_from_rid(self, rid, column, base_page=False):
        '''Given a records rid and column number this function returns tail page value in that specific physical location'''
        page_index, page_slot = self.page_directory[rid][column]
        if (base_page):
            return self.base_pages[column][page_index].get(page_slot)
        return self.tail_pages[column][page_index].get(page_slot)
    
    def __grab_tail_value_from_page_location(self, column, tail_page_location):
        '''Given a tuple of page index and page slot return the value in the tail page of the specific column'''
        return self.tail_pages[column][tail_page_location[0]][tail_page_location[1]]
    
    
    def __merge(self):
        print("Merge is happening")

        while True:
            # Block ensures that we wait for a record to be added to the queue first
            # before we continue merging a record

            rid = self.merge_queue.get(block=True)

            # make a copy of the base page for the recieved rid
            page_range_index, page_index, page_slot = self.get_base_record_location(rid)

            

            self.merge_queue.task_done()
        
    def serialize(self):
        """Returns table metadata as a JSON-compatible dictionary"""
        return {
            "num_columns": self.num_columns,
            "key_index": self.key,
            "page_directory": self.page_directory,
            "rid_index": self.rid_index
        }

