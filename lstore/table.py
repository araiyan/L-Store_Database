from lstore.index import Index
from lstore.page import Page
from time import time
from lstore.config import *

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        '''Each record contains the hidden column as well as the known columns [...HIDDEN_COLUMNS, ...GIVEN_COLUMNS]'''

    def __str__(self):
        return f"RID: {self.rid} Key: {self.key} \nColumns: {self.columns}"

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        if (key < 0 or key >= num_columns):
            raise ValueError("Error Creating Table! Primary Key must be within the columns of the table")

        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.total_num_columns = num_columns + NUM_HIDDEN_COLUMNS
        self.page_directory = {}
        self.index = Index(self)
        
        self.base_pages = {}
        self.tail_pages = {}

        # The table should handle assigning RIDs
        self.rid_index = 0

        for i in range(self.total_num_columns):
            self.base_pages[i] = [Page()]
            self.tail_pages[i] = [Page()]

    def assign_rid_to_record(self, record: Record):
        '''Use this function to assign a record's RID'''
        
        record.rid = self.rid_index
        self.rid_index += 1

    def insert_record(self, record: Record):
        if (self.index.locate(self.key, record.columns[NUM_HIDDEN_COLUMNS + self.key])):
            raise ValueError("Error inserting record to table! Duplicate primary key found")
        
        for i in range(self.total_num_columns):
            if (not self.base_pages[i][-1].has_capacity()):
                self.base_pages[i].append(Page())

            # Points to the last page in the list of pages for the current column
            curr_page:Page = self.base_pages[i][-1] 

            curr_page.write(record.columns[i])
            page_index = curr_page.num_records

            # Each directory entry contains the page# and the index# within that page
            # Note: Subject to change if each column's data isn't an integer
            if (i == RID_COLUMN):
                self.page_directory[record.rid] = [len(self.base_pages[i]) - 1, page_index]

            # TODO: Not sure how the pages are indexed yet 
            pass

    def update_record(self, record: Record):
        origin_rid = self.index.locate(self.key, record.columns[NUM_HIDDEN_COLUMNS + self.key])

        if (origin_rid is None):
            raise ValueError("Error updating record! Record with primary key does not exist")
        
        for i in range(self.total_num_columns):
            if (record.columns[i] is None):
                continue

            if (not self.tail_pages[i][-1].has_capacity()):
                self.tail_pages[i].append(Page())

            curr_page:Page = self.tail_pages[i][-1]

            curr_page.write(record.columns[i])
            page_index = curr_page.num_records

            # TODO: Kind of a hack way of doing it. Need to improve indexing updated columns once pages is solidified
            self.page_directory[record.rid][i] = [len(self.tail_pages[i]) - 1, page_index]

        # updates the indirection column on the base page for new tail page
        page_index, page_slot = self.page_directory[origin_rid]
        self.base_pages[page_index].write_precise(page_slot, record.rid)

    def get_record(self, record_primary_key) -> Record:
        '''Grab the most updated record on the table with matching primary key'''
        origin_rid = self.index.locate(self.key, record_primary_key)
        if (origin_rid is None):
            raise ValueError("Error getting record! Record with the given primary key does not exist")

        page_index, page_slot = self.page_directory[origin_rid]
        record_columns = []

        # Crafting the base page record
        for i in range(self.total_num_columns):
            column_value = self.base_pages[i][page_index].get(page_slot)
            record_columns.append(column_value)

        indirection_rid = record_columns[INDIRECTION_COLUMN]

        # when we have the most updated records records_fill_schema will look like 11111... in binary
        record_fill_schema = 0
        record_fill_max_schema = (2 ** self.num_columns) - 1

        while (indirection_rid != origin_rid and record_fill_schema < record_fill_max_schema):
            record_schema = self.__grab_tail_value(indirection_rid, SCHEMA_ENCODING_COLUMN)
            for i in range(NUM_HIDDEN_COLUMNS, self.total_num_columns):
                # if the record has not been filled
                if (((record_fill_schema >> i) & 1) == 0):
                    # checks schema from last to first going right to left
                    update_bit = (record_schema >> i) & 1

                    if (update_bit):
                        record_fill_schema += (1 << i)
                        record_columns[i] = self.__grab_tail_value(indirection_rid, i)

            indirection_rid = self.__grab_tail_value(indirection_rid, INDIRECTION_COLUMN)

        return Record(origin_rid, self.key, record_columns)
        
    def __grab_tail_value(self, rid, column, base_page=True):
        '''Given a records rid and column number this function returns tail page value in that specific physical location'''
        page_index, page_slot = self.page_directory[rid][column]
        return self.tail_pages[column][page_index].get(page_slot)
    
    def __merge(self):
        print("merge is happening")
        # TODO: Need clarity on merge sequence
        pass
 
