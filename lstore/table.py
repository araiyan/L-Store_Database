from lstore.index import Index
from lstore.page import Page
from time import time
from lstore.config import *
from lstore.bufferpool import Bufferpool
import os

import queue

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
        self.index = Index(self)

        # initialize bufferpool in table, not DB
        self.bufferpool = Bufferpool(self.table_path)
        self.preload_pages()
        
        self.base_pages = {}
        self.tail_pages = {}
        self.tail_pages_prev_merge = [0] * self.num_columns
        '''Keeps track of the number of tail pages that have been merged'''

        self.diallocation_rid_queue = queue.Queue()

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

            page_index = curr_page.num_records
            curr_page.write(record.columns[i])
            

            # Each directory entry contains the page# and the index# within that page
            # Note: Subject to change if each column's data isn't an integer
            if (i == RID_COLUMN):
                self.page_directory[record.rid] = [(len(self.base_pages[i]) - 1, page_index)]
                

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

        # TODO: Query update should implement this


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
            record_schema = self.__grab_tail_value_from_rid(indirection_rid, SCHEMA_ENCODING_COLUMN)
            for i in range(NUM_HIDDEN_COLUMNS, self.total_num_columns):
                # if the record has not been filled
                if (((record_fill_schema >> i) & 1) == 0):
                    # checks schema from last to first going right to left
                    update_bit = (record_schema >> i) & 1

                    if (update_bit):
                        record_fill_schema += (1 << i)
                        record_columns[i] = self.__grab_tail_value_from_rid(indirection_rid, i)

            indirection_rid = self.__grab_tail_value_from_rid(indirection_rid, INDIRECTION_COLUMN)

        return Record(origin_rid, self.key, record_columns)
    
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

        # Here were using rid column because it will always get filled out first
        if (len(self.tail_pages[RID_COLUMN]) - self.tail_pages_prev_merge[RID_COLUMN]) < MAX_TAIL_PAGES_BEFORE_MERGING:
            return False
            
        # Grabs all records rid
        # TODO: If each indicy is a binary tree we need to iterate through the binary tree and grab all rids
        all_rids = self.index.indices[self.key].grab_all()

        # for each rid grab the most updated columns
        for _, base_rid in enumerate(all_rids):
            # get the base page of the record
            page_index, page_slot = self.page_directory[base_rid][0]
            print(f"Merge -> page index: {page_index} page slot: {page_slot}")

            indirection_rid = self.base_pages[INDIRECTION_COLUMN][page_index].get(page_slot)
            column_page_locations = self.page_directory[indirection_rid]

            # iterate through all the consolidated pages
            while (indirection_rid != base_rid and len(column_page_locations) == 1):
                page_index, page_slot = column_page_locations[0]
                indirection_rid = self.base_pages[INDIRECTION_COLUMN][page_index].get(page_slot)
                column_page_locations = self.page_directory[indirection_rid]

            # form our consolidated record
            consolidated_record = Record(0, self.key, [0] * self.total_num_columns)
            self.assign_rid_to_record(consolidated_record)

            latest_base_page_index = page_index
            latest_base_page_slot = page_slot

            # have the consolidated indirection_rid point towards the latest tail record
            consolidated_indirection_rid = indirection_rid

            for i in range(self.num_columns):
                consolidated_record.columns[NUM_HIDDEN_COLUMNS + i] = self.base_pages[i + NUM_HIDDEN_COLUMNS][page_index].get(page_slot)

            consolidated_time_stamp = self.base_pages[TIMESTAMP_COLUMN][page_index].get(page_slot)

            # when we have the most updated records records_fill_schema will look like 11111... in binary
            record_fill_schema = (2 ** self.key) # flipping key schema bit since key is never updated
            record_fill_max_schema = (2 ** self.num_columns) - 1

            while (indirection_rid != base_rid and record_fill_schema < record_fill_max_schema):
                column_page_locations = self.page_directory[indirection_rid]
                if len(column_page_locations) < self.total_num_columns:
                    raise ValueError("Error Inside Merge: could not locate tail page. Current page locations: ", column_page_locations)
                
                tail_page_time_stamp = self.__grab_tail_value_from_page_location(TIMESTAMP_COLUMN, column_page_locations[TIMESTAMP_COLUMN])

                # if tail page is not updated we have the most updated data
                if (tail_page_time_stamp < consolidated_time_stamp):
                    break

                tail_page_schema = self.__grab_tail_value_from_page_location(SCHEMA_ENCODING_COLUMN, column_page_locations[SCHEMA_ENCODING_COLUMN])

                for i in range(self.num_columns):
                    # if the record has not been filled
                    if (((record_fill_schema >> i) & 1) == 0):
                        # checks schema from last to first going right to left
                        update_bit = (tail_page_schema >> i) & 1

                        if (update_bit):
                            record_fill_schema += (1 << i)
                            consolidated_record.columns[-(i+1)] = self.__grab_tail_value_from_page_location(-(i+1), column_page_locations[-(i+1)])

                indirection_rid = self.__grab_tail_value_from_page_location(INDIRECTION_COLUMN, column_page_locations[INDIRECTION_COLUMN])

            
            # Now we have the most consolidated record for the rid
            consolidated_record.columns[INDIRECTION_COLUMN] = consolidated_indirection_rid
            consolidated_record.columns[SCHEMA_ENCODING_COLUMN] = 0
            consolidated_record.columns[RID_COLUMN] = consolidated_record.rid
            consolidated_record.columns[TIMESTAMP_COLUMN] = time()

            # Insert the consolidated column into a consolidated base page
            for i in range(self.total_num_columns):
                if (not self.base_pages[i][-1].has_capacity()):
                    self.base_pages[i].append(Page())

                # Points to the last page in the list of pages for the current column
                curr_page:Page = self.base_pages[i][-1] 
                curr_page.write(consolidated_record.columns[i])
                

                if (i == RID_COLUMN):
                    self.page_directory[consolidated_record.rid] = [(len(self.base_pages[i]) - 1, curr_page.num_records - 1)]

            # have the base_page or latest consolidated_page point to the new consolidated record
            self.base_pages[INDIRECTION_COLUMN][latest_base_page_index].write_precise(latest_base_page_slot, consolidated_record.rid)

        # update the merged tails page index
        for i in range(self.total_num_columns):
            self.tail_pages_prev_merge[i] = len(self.tail_pages[i]) - 1
        
    def serialize(self):
        """Returns table metadata as a JSON-compatible dictionary"""
        return {
            "num_columns": self.num_columns,
            "key_index": self.key,
            "page_directory": self.page_directory,
            "indexes": {
                col: self.index.indices[col] for col in range(self.num_columns) if self.index.indices[col]
            }
        }
    
    def preload_pages(self):
        """Preloads pages into Bufferpool when table is initialized"""
        if os.path.exists(self.table_path):
            for page_range in os.listdir(self.table_path):
                page_range_path = os.path.join(self.table_path, page_range)
                if os.path.isdir(page_range_path) and page_range.startswith("PageRange_"):
                    page_range_index = int(page_range.split("_")[1])

                    for file in os.listdir(page_range_path):
                        if file.startswith("Page_") and file.endswith(".bin"):
                            parts = file.split("_")
                            column_index = int(parts[1])
                            page_index = int(parts[2].split(".")[0])

                            frame = self.bufferpool.get_page_frame(page_range_index, column_index, page_index)

                            if frame:
                                print(f"Preloaded {file} into bufferpool for table {self.name}")
                            else:
                                raise ValueError(f"Could not preload {file} (Bufferpool full or error)")

