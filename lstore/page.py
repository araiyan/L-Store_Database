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

    