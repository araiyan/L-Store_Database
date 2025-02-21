from lstore.config import *
from lstore.bufferpool import BufferPool
import json

class PageRange:
    '''
    Each PageRange contains all columns of a record
    Indirection column of a base page would contain the logical_rid of its corresponding tail record
    '''

    def __init__(self, page_range_index, num_columns, bufferpool:BufferPool):
        self.bufferpool = bufferpool
        self.logical_directory = {}
        '''Maps logical rid's to physical locations in page for each column (except hidden columns)'''
        self.logical_rid_index = MAX_PAGE_RANGE * MAX_RECORD_PER_PAGE
        '''Used to assign logical rids to updates'''

        self.total_num_columns = num_columns + NUM_HIDDEN_COLUMNS
        '''Total number of columns in a record'''

        self.tail_page_index = [MAX_PAGE_RANGE] * self.total_num_columns
        '''Tail page index for each column'''

        self.tps = 0
        '''Tail page sequence number'''

        self.page_range_index = page_range_index

    def write_base_record(self, page_index, page_slot, *columns) -> bool:
        for (i, column) in enumerate(columns):
            self.bufferpool.write_page_slot(self.page_range_index, i, page_index, page_slot, column)
        return True
    
    def copy_base_record(self, page_index, page_slot) -> list:
        '''Costly function mostly used by table merge on the background'''
        base_record_columns = [None] * self.total_num_columns
        # Read buffer pool frames
        for i in range(self.total_num_columns):
            base_record_columns = self.bufferpool.read_page_slot(self.page_range_index, i, page_index, page_slot)

        # mark the frame used after reading
        for i in range(self.total_num_columns):
            frame_num = self.bufferpool.get_page_frame_num(self.page_range_index, i, page_index, page_slot)
            self.bufferpool.mark_frame_used(frame_num)

        return base_record_columns
        

    def write_tail_record(self, logical_rid, *columns) -> bool:
        '''Writes a set of columns to the tail pages returns true on success'''

        self.logical_directory[logical_rid] = [None] * (self.total_num_columns - NUM_HIDDEN_COLUMNS)

        for (i, column) in enumerate(columns):
            # If current tail page doesn't have capacity move to the next tail page
            has_capacity =  self.bufferpool.get_page_has_capacity(self.page_range_index, i, self.tail_page_index[i])

            if not has_capacity:
                self.tail_page_index[i] += 1
            elif has_capacity is None:
                return False
                
            page_slot = self.bufferpool.write_page_next(self.page_range_index, i, self.tail_page_index[i], column)

            # we can skip mapping hidden columns since they are partitioned equally
            if (i >= NUM_HIDDEN_COLUMNS):
                self.logical_directory[logical_rid][i] = (self.tail_page_index[i] * MAX_RECORD_PER_PAGE) + page_slot

        return True
    
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
        physical_rid = self.logical_directory[logical_rid][column]
        page_index = physical_rid // MAX_RECORD_PER_PAGE
        page_slot = physical_rid % MAX_RECORD_PER_PAGE
        return page_index, page_slot

    def has_capacity(self, rid) -> bool:
        '''returns true if there is capacity in the base pages for the given rid '''
        return rid < (self.page_range_index * MAX_PAGE_RANGE * MAX_RECORD_PER_PAGE) 

    def assign_logical_rid(self) -> int:
        '''returns logical rid to be assigned to a column'''
        self.logical_rid_index += 1
        return self.logical_rid_index
    
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
        self.logical_directory = json_data["logical_directory"]
        self.tail_page_index = json_data["tail_page_index"]
        self.logical_rid_index = json_data["logical_rid_index"]
        self.tps = json_data["tps"]

    def __hash__(self):
        return self.page_range_index
    
    def __eq__(self, other):
        return self.page_range_index == other.page_range_index
    
    def __str__(self):
        return json.dumps(self.serialize())
    
    

