from lstore.config import PAGE_SIZE

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        pass

    def write(self, value):
        self.num_records += 1
        pass

    def write_precise(self, index, value):
        '''
        This function should be able to write data on a precise index inside the table.
        Useful for changing the indirection column on the base page
        '''
        pass

    def get(self, index):
        '''This funciton should be able to grab a data located at a certain index in the page'''
        pass


class PageRange:
    '''
    Each PageRange all columns of a record
    Indirection column of a base page would contain the logical_rid of its corresponding tail record
    '''

    def __init__(self, page_range_size):
        self.base_pages = {}
        self.tail_pages = {}
        self.logical_directory = {}
        '''Maps logical rid's to tail pages'''
        self.logical_rid_index = 0
        '''Used to assign logical rids to updates'''
        self.page_range_size = page_range_size
        '''Max Number of Base Pages inside one PageRange'''

    def write(self, value) -> tuple[int, int]:
        '''
        Given a value write to the base page then 
        return the base_page index, and the page_index 
        '''
        pass

    def assign_logical_rid(self) -> int:
        '''returns logical rid to be assigned to a column'''
        pass

