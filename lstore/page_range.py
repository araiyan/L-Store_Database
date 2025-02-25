from lstore.config import *
from lstore.bufferpool import BufferPool
import json
from typing import Type
import queue

class MergeRequest:
    '''Contains information about the tail pages to be merged'''
    def __init__(self, page_range_index):
        self.page_range_index = page_range_index

class PageRange:
    '''
    Each PageRange contains all columns of a record
    Indirection column of a base page would contain the logical_rid of its corresponding tail record
    '''

    def __init__(self, page_range_index, num_columns, bufferpool:BufferPool, merge_queue:queue.Queue):
        self.bufferpool = bufferpool
        self.merge_queue = merge_queue
        self.logical_directory = {}
        '''Maps logical rid's to physical locations in page for each column (except hidden columns)'''
        self.logical_rid_index = MAX_RECORD_PER_PAGE_RANGE
        '''Used to assign logical rids to updates'''

        self.total_num_columns = num_columns + NUM_HIDDEN_COLUMNS
        '''Total number of columns in a record'''

        self.tail_page_index = [MAX_PAGE_RANGE] * self.total_num_columns
        '''Tail page index for each column'''

        self.tps = 0
        '''Tail page sequence number'''

        self.page_range_index = page_range_index

        '''setup queues for logical rid allocation/deallocation'''
        self.deallocation_logical_rid_queue = queue.Queue()
        self.allocation_logical_rid_queue = queue.Queue()

    def write_base_record(self, page_index, page_slot, columns) -> bool:
        columns[INDIRECTION_COLUMN] = self.__normalize_rid(columns[RID_COLUMN])
        for (i, column) in enumerate(columns):
            self.bufferpool.write_page_slot(self.page_range_index, i, page_index, page_slot, column)
        return True
    
    def copy_base_record(self, page_index, page_slot) -> list:
        '''Costly function mostly used by table merge on the background'''
        base_record_columns = [None] * self.total_num_columns
        # Read buffer pool frames
        for i in range(self.total_num_columns):
            base_record_columns[i] = self.bufferpool.read_page_slot(self.page_range_index, i, page_index, page_slot)

        # mark the frame used after reading
        for i in range(self.total_num_columns):
            frame_num = self.bufferpool.get_page_frame_num(self.page_range_index, i, page_index)
            self.bufferpool.mark_frame_used(frame_num)

        return base_record_columns
    
    def find_records_last_logical_rid(self, logical_rid):
        '''Merge Helper API Call: Returns the last logical rid of a record given a starting logical rid'''
        last_logical_rid = logical_rid
        while (logical_rid >= MAX_RECORD_PER_PAGE_RANGE):
            last_logical_rid = logical_rid
            page_index, page_slot = self.get_column_location(logical_rid, INDIRECTION_COLUMN)
            logical_rid = self.bufferpool.read_page_slot(self.page_range_index, INDIRECTION_COLUMN, page_index, page_slot)
            frame_num = self.bufferpool.get_page_frame_num(self.page_range_index, INDIRECTION_COLUMN, page_index)
            self.bufferpool.mark_frame_used(frame_num)

        return last_logical_rid

    def write_tail_record(self, logical_rid, *columns) -> bool:
        '''Writes a set of columns to the tail pages returns true on success'''

        self.logical_directory[logical_rid] = [None] * (self.total_num_columns - NUM_HIDDEN_COLUMNS)

        for (i, column) in enumerate(columns):
            if (column is None):
                continue
            
            # If current tail page doesn't have capacity move to the next tail page
            has_capacity =  self.bufferpool.get_page_has_capacity(self.page_range_index, i, self.tail_page_index[i])

            if not has_capacity:
                self.tail_page_index[i] += 1

            elif has_capacity is None:
                return False
                
            page_slot = self.bufferpool.write_page_next(self.page_range_index, i, self.tail_page_index[i], column)

            # we can skip mapping hidden columns since they are partitioned equally
            if (i >= NUM_HIDDEN_COLUMNS):
                self.logical_directory[logical_rid][i - NUM_HIDDEN_COLUMNS] = (self.tail_page_index[i] * MAX_RECORD_PER_PAGE) + page_slot

        self.tps += 1
        if (self.tps % (MAX_TAIL_PAGES_BEFORE_MERGING * MAX_RECORD_PER_PAGE) == 0):
            self.merge_queue.put(MergeRequest(self.page_range_index))

        return True
    
    def read_tail_record_column(self, logical_rid, column) -> int:
        '''Reads a column from the tail pages given a logical rid'''
        page_index, page_slot = self.get_column_location(logical_rid, column)
        column_value = self.bufferpool.read_page_slot(self.page_range_index, column, page_index, page_slot)
        frame_num = self.bufferpool.get_page_frame_num(self.page_range_index, column, page_index)
        self.bufferpool.mark_frame_used(frame_num)
        return column_value
    
    # Only use this function for API calls
    def get_column_location(self, logical_rid, column) -> tuple[int, int]:
        '''Returns the location of a column within tail pages given a logical rid'''
        if (column < NUM_HIDDEN_COLUMNS):
            return self.__get_hidden_column_location(logical_rid)
        else:
            return self.__get_known_column_location(logical_rid, column)
    
    def __get_hidden_column_location(self, logical_rid) -> tuple[int, int]:
        '''Returns the location of the hidden columns for a logical rid'''
        page_index = logical_rid // MAX_RECORD_PER_PAGE
        page_slot = logical_rid % MAX_RECORD_PER_PAGE
        return page_index, page_slot
    
    def __get_known_column_location(self, logical_rid, column) -> tuple[int, int]:
        '''Returns the location of a column within tail pages given a logical rid'''
        physical_rid = self.logical_directory[logical_rid][column - NUM_HIDDEN_COLUMNS]
        page_index = physical_rid // MAX_RECORD_PER_PAGE
        page_slot = physical_rid % MAX_RECORD_PER_PAGE
        return page_index, page_slot
    
    def __normalize_rid(self, rid) -> int:
        '''Returns the normalized rid for a given rid'''
        return rid % MAX_RECORD_PER_PAGE_RANGE

    def has_capacity(self, rid) -> bool:
        '''returns true if there is capacity in the base pages for the given rid '''
        return rid < (self.page_range_index * MAX_PAGE_RANGE * MAX_RECORD_PER_PAGE) 

    def assign_logical_rid(self) -> int:
        '''returns logical rid to be assigned to a column'''
        if not self.allocation_logical_rid_queue.empty():
            return self.allocation_logical_rid_queue.get()
        else:
            self.logical_rid_index += 1
            return self.logical_rid_index - 1
    
    def serialize(self):
        '''Returns page metadata as a JSON-compatible dictionary'''
        return {
            "logical_directory": self.logical_directory,
            "tail_page_index": self.tail_page_index,
            "logical_rid_index": self.logical_rid_index,
            "tps": self.tps
        }
    
    def deserialize(self, json_data):
        '''Loads a page from serialized data'''
        self.logical_directory = {int(k): v for k, v in json_data["logical_directory"].items()}
        self.tail_page_index = json_data["tail_page_index"]
        self.logical_rid_index = json_data["logical_rid_index"]
        self.tps = json_data["tps"]

    def __hash__(self):
        return self.page_range_index
    
    def __eq__(self, other:Type['PageRange']):
        return self.page_range_index == other.page_range_index
    
    def __str__(self):
        return json.dumps(self.serialize())

    def __repr__(self):
        return json.dumps(self.serialize())
    
    

