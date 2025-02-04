from lstore.config import *
import struct

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < MAX_RECORD_PER_PAGE

    def write(self, value):
        struct.pack_into("i", self.data, self.num_records * INTEGER_BYTE_SIZE, value)
        self.num_records += 1
        return (self.num_records - 1)

    def write_precise(self, index, value):
        '''
        This function should be able to write data on a precise index inside the table.
        Useful for changing the indirection column on the base page
        '''
        struct.pack_into("i", self.data, index * INTEGER_BYTE_SIZE, value)

    def get(self, index):
        '''This funciton should be able to grab a data located at a certain index in the page'''
        return struct.unpack_from("i", self.data, index * INTEGER_BYTE_SIZE)[0]


class PageRange:
    '''
    Each PageRange contains all columns of a record
    Indirection column of a base page would contain the logical_rid of its corresponding tail record
    '''

    def __init__(self, num_columns):
        self.base_pages = {}
        self.consolidated_pages = {}
        self.tail_pages = {}
        self.logical_directory = {}
        '''Maps logical rid's to consolidated/tail pages'''
        self.logical_rid_index = MAX_PAGE_RANGE * MAX_RECORD_PER_PAGE
        '''Used to assign logical rids to updates'''

        self.total_num_columns = num_columns + NUM_HIDDEN_COLUMNS

        for i in range(self.total_num_columns):
            self.base_pages[i] = [Page()]
            self.consolidated_pages[i] = [Page()]
            self.tail_pages[i] = [Page()]

    def write(self, columns) -> tuple[int, int]:
        '''
        Insert a set of columns to the base page then
        return the base_page_index, and the page_slot 
        '''
        if (len(columns) != self.total_num_columns):
            raise IndexError("Columns must contain all hidden and given columns")
        
        
        base_page_slot = 0
        
        for i in range(self.total_num_columns):
            latest_base_page:Page = self.base_pages[i][-1]
            if not latest_base_page.has_capacity():
                self.base_pages[i].append(Page())
                latest_base_page = self.base_pages[i][-1]
            
            base_page_slot = latest_base_page.write(columns[i])

        base_page_index = len(self.base_pages[RID_COLUMN]) - 1
        return base_page_index, base_page_slot

    def has_capacity(self):
        return len(self.base_pages[RID_COLUMN]) < MAX_PAGE_RANGE or self.base_pages[RID_COLUMN][-1].has_capacity()

    def assign_logical_rid(self) -> int:
        '''returns logical rid to be assigned to a column'''
        self.logical_rid_index += 1
        return self.logical_rid_index

