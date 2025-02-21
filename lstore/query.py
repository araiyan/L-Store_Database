from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from lstore.config import *
from lstore.bufferpool import Bufferpool, Frame

from time import time


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table, bufferpool):
        self.table:Table = table
        self.bufferpool:Bufferpool = bufferpool
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):

        # Locate the RID associated with the primary key
        base_rid = self.table.index.locate(self.table.key, primary_key)
        if(base_rid is None):
            return False  # Record does not exist

        base_page_index, base_page_slot = self.table.page_directory[base_rid[0]][0]
        page_range_index = None # Somehow get a page range index where the base record is stored; currently just using None as placeholder

        # Use the Bufferpool API to load the appropriate frame.
        tup = self.__getPageTuple(page_range_index, RID_COLUMN, base_page_index)
        
        frame, frame_num = tup

        frame.write_precise_with_lock(base_page_slot, RECORD_DELETION_FLAG)
        self.table.diallocation_rid_queue.put(base_rid[0])
        self.table.index.delete_from_index(self.table.key, primary_key, base_rid[0])

        # Mark frame as used, no longer needed
        self.bufferpool.mark_frame_used(frame_num)              # Decreases pin count on frame

        # Deletion successful
        return True

    

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # check for invalid number of columns
        if len(columns) != self.table.num_columns:
            return False
  
        record = Record(rid = None, key = self.table.key, columns = None)
        self.table.assign_rid_to_record(record)

        hidden_columns = [None] * NUM_HIDDEN_COLUMNS
        hidden_columns[INDIRECTION_COLUMN] = record.rid
        hidden_columns[RID_COLUMN] = record.rid
        hidden_columns[TIMESTAMP_COLUMN] = int(time())
        hidden_columns[SCHEMA_ENCODING_COLUMN] = 0
        record.columns = hidden_columns + list(columns)
        
        self.table.insert_record(record)
        self.table.index.insert_to_index(self.table.key, columns[self.table.key], record.rid)

        return True

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        # retrieve a list of RIDs that contain the "search_key" value within the column as defined by "search_key_index"
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):

        # retrieve a list of RIDs that contain the "search_key" value within the column as defined by "search_key_index"
        rid_list = self.table.index.locate(search_key_index, search_key)

        # if there exists no RIDs that match the given parameters, return False
        if not rid_list:
            raise ValueError("No records found with the given key")

        record_objs = []

        # iterate through all desired RIDs
        for rid in rid_list:
            record_columns = [None] * self.table.num_columns
            projected_columns_schema = 0
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 1:
                    projected_columns_schema |= (1 << i)

            # Since primary key is never updated we can grab the primary key column directly from the base page
            if (projected_columns_schema >> self.table.key) & 1 == 1:
                base_page_number, base_page_index = self.table.page_directory[rid][0]
                page_range_index = None # Currently not a way to get it, temporarily replacing w/ None

                page_tuple = self.__getPageTuple(page_range_index, NUM_HIDDEN_COLUMNS + self.table.key, base_page_index)               # Increment primary key pin

                frame, frame_num = page_tuple
                record_columns[self.table.key] = frame.page.get(base_page_number)
                projected_columns_schema &= ~(1 << self.table.key)
                self.bufferpool.mark_frame_used(frame_num)                                                                              # Decrement primary key pin
            
            consolidated_stack, indirection_rid = self.__grabConsolidatedStack(rid)
            # store the (base) page# and index# that the RID/row is located

            consolidated_rid, consolidated_timestamp = consolidated_stack.pop()

            # print(f"RID: {rid}, Base RID: {consolidated_rid}, Timestamp: {consolidated_timestamp}, Indirection RID: {indirection_rid}")

            # traverse through the tail pages based on the relative version
            current_version = 0
            page_locations = self.table.page_directory[indirection_rid]
            
            while (indirection_rid != rid and current_version > relative_version):
                current_version -= 1
                


                # I think page_locations[0] represents the page index and page_locations[1] represents the slot? 

                """
                This part is wrong since we shouldn't be geting the page index using page_locations[0] as we're looking for the page range instead.
                """

                indir_page_range = None
                tail_page_range = None

                indir_tuple = self.__getPageTuple(indir_page_range, INDIRECTION_COLUMN, page_locations[1])            # Increment indirection pin 
                tail_tuple  = self.__getPageTuple(tail_page_range, TIMESTAMP_COLUMN, page_locations[1])               # Increment tail pin
                
                # From the tuples returned by the bufferpool, we extract the pages where the indirection and tail are stored
                indir_frame, indir_frame_num = indir_tuple
                tail_frame, tail_frame_num = tail_tuple
                
                indirection_rid = indir_frame.page.get(page_locations[1])
                tail_timestamp = tail_frame.page.get(page_locations[1])

                # Skip over newer consolidated pages
                if (consolidated_timestamp > tail_timestamp):
                    consolidated_rid, consolidated_timestamp = consolidated_stack.pop()

                page_locations = self.table.page_directory[indirection_rid]

                self.bufferpool.mark_frame_used(indir_frame_num)            # Decrement indirection pin
                self.bufferpool.mark_frame_used(tail_frame_num)             # Decrement tail pin

            #print("Consolidated stack", consolidated_stack)
            #print("page Locations", page_locations)
            
            if (len(page_locations) > 1):
                while (projected_columns_schema > 0 and indirection_rid != rid and tail_timestamp >= consolidated_timestamp):
                    tail_page_range = None
                    schema_page_range = None

                    tail_tuple = self.__getPageTuple(tail_page_range, TIMESTAMP_COLUMN, page_locations[1])              # Increment tail pin
                    schema_tuple = self.__getPageTuple(schema_page_range, SCHEMA_ENCODING_COLUMN, page_locations[1])      # Increment schema pin
                    
                    tail_frame, tail_frame_num = tail_tuple
                    schema_frame, schema_frame_num = schema_tuple

                    tail_timestamp = tail_frame.page.get(page_locations[1])
                    schema_column = schema_frame.page.get(page_locations[1])

                    for i in range(self.table.num_columns):
                        if (((projected_columns_schema >> i) & 1 == 1) and ((schema_column >> i) & 1 == 1)):
                            col_page_range = None

                            col_tuple = self.__getPageTuple(col_page_range, NUM_HIDDEN_COLUMNS + i, page_locations[1])      # Increment record column pin
                            
                            col_frame, col_frame_num = col_tuple
                            record_columns[i] = col_frame.page.get(page_locations[1])
                            projected_columns_schema &= ~(1 << i)
                            self.bufferpool.mark_frame_used(col_frame_num)              # Decrement record column pin

                        if projected_columns_schema == 0:
                            break

                    indir_page_range = None
                    indir_tuple = self.__getPageTuple(indir_page_range, INDIRECTION_COLUMN, page_locations[1])               # Incrememnt indir pin
                    indir_frame, indir_frame_num = indir_tuple
                    indirection_rid = indir_frame.page.get(page_locations[1])
                    page_locations = self.table.page_directory[indirection_rid]

                    self.bufferpool.mark_frame_used(indir_frame_num)                        # Decrement indir pin
                    self.bufferpool.mark_frame_used(tail_frame_num)                         # Decrement tail pin
                    self.bufferpool.mark_frame_used(schema_frame_num)                       # Decrement schema pin

            # if we were unsuccessful in finding an older version - this implies that indirection_rid == rid and we can
            # thus directly retrieve from the base/consolidated page

            consolidated_page_location = self.table.page_directory.get(consolidated_rid, None)
            consolidated_page_index, consolidated_page_slot = consolidated_page_location[0]
            consolidated_page_range = None  # Temporarily NONE


            for i in range(self.table.num_columns):
                if (projected_columns_schema >> i) & 1 == 1:

                    page_range = None
                    page_tuple = self.__getPageTuple(page_range, NUM_HIDDEN_COLUMNS + i, consolidated_page_index)      # Increment record column pin
                    
                    frame, frame_num = page_tuple
                    record_columns[i] = frame.page.get(consolidated_page_slot)
                    projected_columns_schema &= ~(1 << i)
                    self.bufferpool.mark_frame_used(frame_num)                          # Decrement record column pin
                
            record_objs.append(Record(rid, record_columns[self.table.key], record_columns))
            
        return record_objs
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking (Ignore this for now)
    """
    def update(self, primary_key, *columns):

        # We want to see if the record exists with the specified key; no point in continuing further without checking
        rid_location = self.table.index.locate(self.table.key, primary_key)
        if(rid_location is None):
            print("Update Error: Record does not exist")
            return False
        
        # The tail should have the same number of columns as the other pages so we should be multiplying by the total_num_columns value
        # Multiplying by [None] atm since the columns don't have an assigned size
        new_columns = [None] * self.table.total_num_columns

        schema_encoding = 0
        projected_columns = []

        # Insert values into the new column
        for i, value in enumerate(columns):

            # If we're modifying the primary_key then this update should be stopped since we can't change the primary_key column
            if(i == self.table.key and value is not None):
                raise KeyError("Primary key cannot be updated")
            
            if(value is not None):
                schema_encoding |= (1 << i)
                projected_columns.append(1)
            else:
                projected_columns.append(0)
            
            new_columns[NUM_HIDDEN_COLUMNS + i] = value

        # We want to set indirection for temp tail record to be the previous tail_rid --> go to base/cons to get latest
        cons_rid = self.__getLatestConRid(rid_location[0])
        cons_page_index, cons_page_slot = self.table.page_directory[cons_rid][0]
        
        tail_tuple = self.__getPageTuple(None, INDIRECTION_COLUMN, cons_page_index)
        tail_frame, frame_num = tail_tuple
        prev_tail_rid = tail_frame.page.get(cons_page_slot)
        self.bufferpool.mark_frame_used(frame_num)

        new_columns[INDIRECTION_COLUMN] = prev_tail_rid
        new_columns[SCHEMA_ENCODING_COLUMN] = schema_encoding
        new_columns[TIMESTAMP_COLUMN] = int(time())

        # Updates the indirection column on the base/cons page for new tail page
        cons_page_index, cons_page_slot = self.table.page_directory[cons_rid][0]
        indir_tuple = self.__getPageTuple(None, INDIRECTION_COLUMN, cons_page_index)
        frame, frame_num = indir_tuple

        # Create new record and initialize it into the pd
        new_record = Record(rid = -1, key = primary_key, columns = new_columns)
        self.table.assign_rid_to_record(new_record)
        self.table.page_directory[new_record.rid] = [None] * self.table.total_num_columns
        new_columns[RID_COLUMN] =  new_record.rid
        self.__writeTailRecord(new_record)
        
        frame.write_precise_with_lock(cons_page_slot, new_record.rid)
        self.bufferpool.mark_frame_used(frame_num)

        # Update successful
        # print("Update Successful\n")
        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return self.sum_version(start_range, end_range, aggregate_column_index, 0)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):

        records_list = self.table.index.locate_range(start_range, end_range, self.table.key)

        if records_list is None:
            return False

        sum_total = 0
        for rid in records_list:
            aggregate_column_value = None
            consolidated_stack, indirection_rid = self.__grabConsolidatedStack(rid)
            # store the (base) page# and index# that the RID/row is located

            consolidated_rid, consolidated_timestamp = consolidated_stack.pop()

            # print(f"RID: {rid}, Base RID: {consolidated_rid}, Timestamp: {consolidated_timestamp}, Indirection RID: {indirection_rid}")

            # traverse through the tail pages based on the relative version
            current_version = 0
            page_locations = self.table.page_directory[indirection_rid]
            
            while (indirection_rid != rid and current_version > relative_version):
                current_version -= 1
                
                indir_page_range = None
                tail_page_range = None

                indir_tuple = self.__getPageTuple(indir_page_range, INDIRECTION_COLUMN, page_locations[1])
                tail_tuple = self.__getPageTuple(tail_page_range, TIMESTAMP_COLUMN, page_locations[1])

                indir_frame, indir_frame_num = indir_tuple
                indirection_rid = indir_frame.page.get(page_locations[1])
                self.bufferpool.mark_frame_used(indir_frame_num)

                tail_frame, tail_frame_num = tail_tuple
                tail_timestamp = tail_frame.page.get(page_locations[1])
                self.bufferpool.mark_frame_used(tail_frame_num)

                # Skip over newer consolidated pages
                if (consolidated_timestamp > tail_timestamp):
                    consolidated_rid, consolidated_timestamp = consolidated_stack.pop()

                page_locations = self.table.page_directory[indirection_rid]

            #print("Consolidated stack", consolidated_stack)
            #print("page Locations", page_locations)
            
            if (len(page_locations) > 1):

                tail_page_range = None
                tail_tuple = self.__getPageTuple(tail_page_range, TIMESTAMP_COLUMN, page_locations[1])
                tail_frame, frame_num = tail_tuple
                tail_timestamp = tail_frame.page.get(page_locations[1])
                self.bufferpool.mark_frame_used(tail_frame_num)

                while (indirection_rid != rid and tail_timestamp >= consolidated_timestamp and aggregate_column_value is None):
                    
                    tail_page_range = None
                    tail_tuple = self.__getPageTuple(tail_page_range, TIMESTAMP_COLUMN, page_locations[1])
                    tail_frame, tail_frame_num = tail_tuple
                    tail_timestamp = tail_frame.page.get(page_locations[1])
                    
                    schema_page_range = None
                    schema_tuple = self.__getPageTuple(schema_page_range, SCHEMA_ENCODING_COLUMN, page_locations[1])
                    schema_frame, schema_frame_num = schema_tuple
                    schema_column = schema_frame.page.get(page_locations[1])

                    self.bufferpool.mark_frame_used(schema_frame_num)
                    self.bufferpool.mark_frame_used(tail_frame_num)
                    
                    if (((schema_column >> aggregate_column_index) & 1 == 1)):
                        
                        col_page_range = None
                        col_tuple = self.__getPageTuple(col_page_range, NUM_HIDDEN_COLUMNS + aggregate_column_index, page_locations[1])
                        col_frame, col_frame_num = col_tuple
                        aggregate_column_value = col_frame.page.get(page_locations[1])
                        self.bufferpool.mark_frame_used(col_frame_num)

                    
                    indir_page_range = None
                    indir_tuple = self.__getPageTuple(indir_page_range, INDIRECTION_COLUMN, page_locations[1])
                    indir_frame, indir_frame_num = indir_tuple
                    indirection_rid = indir_frame.page.get(page_locations[1])
                    self.bufferpool.mark_frame_used(indir_frame_num)

                    page_locations = self.table.page_directory[indirection_rid]

            # if we were unsuccessful in finding an older version - this implies that indirection_rid == rid and we can
            # thus directly retrieve from the base/consolidated page

            if (aggregate_column_value is None):
                consolidated_page_location = self.table.page_directory.get(consolidated_rid, None)
                consolidated_page_index, consolidated_page_slot = consolidated_page_location[0]
                tup = self.__getPageTuple(None, NUM_HIDDEN_COLUMNS + aggregate_column_index, consolidated_page_index)
                frame, frame_num = tup
                aggregate_column_value = frame.page.get(consolidated_page_slot)
                self.bufferpool.mark_frame_used(frame_num)
                    
            sum_total += aggregate_column_value
        
        return sum_total

    
    """
    Increments one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
    

    """
    Starting from the base record’s indirection pointer, we want to go through all the 
    base/consolidated records. Iterate through with until the column_page_locations tuple
    is greater than 1, signifying the end of the base/consolidated records
    """
    def __getLatestConRid(self, base_rid):

        # We are given the base rid so we want to start from here and iterate through all the consolidated pages if there are any
        base_page_index, base_page_slot = self.table.page_directory[base_rid][0]

        indir_tuple = self.__getPageTuple(None, INDIRECTION_COLUMN, base_page_index)
        indir_frame, frame_num = indir_tuple
        indir_rid = indir_frame.page.get(base_page_slot)
        
        # If no cons; newest is base
        previous_rid = base_rid

        ## This is moreso just for checking tuple length to ensure we're looking at a base/consolidated page
        column_page_locations = self.table.page_directory[indir_rid]

        self.bufferpool.mark_frame_used(frame_num)

        # If there are consolidated pages w/ we walk them for indirection ptr that points to the latest tail
        while(indir_rid != base_rid and len(column_page_locations) == 1):
            
            previous_rid = indir_rid
            page_index, page_slot = column_page_locations[0]
            
            indir_tuple = self.__getPageTuple(None, INDIRECTION_COLUMN, page_index)
            indir_frame, frame_num = indir_tuple
            indir_rid = indir_frame.page.get(page_slot)
            
            column_page_locations = self.table.page_directory[indir_rid]
            self.bufferpool.mark_frame_used(frame_num)

        return previous_rid

    

    """Helper function that writes the new record into the appropriate tail pages."""
    def __writeTailRecord(self, new_record):

        for i in range(self.table.total_num_columns):
            
            # Skip columns we're not updating
            if(new_record.columns[i] is None):
                continue

            # If current tail page is full, append a new page
            if(not self.table.tail_pages[i][-1].has_capacity()):
                self.table.tail_pages[i].append(Page())

            tail_page_index = len(self.table.tail_pages[i]) - 1
            tail_tuple = self.__getPageTuple(None, NUM_HIDDEN_COLUMNS + i, tail_page_index)
            tail_frame, frame_num = tail_tuple

            # We don't really care where this tail record goes so we don't need to use precise_with_lock()
            pos = tail_frame.write_with_lock(new_record.columns[i])

            # Write this to the pd
            self.table.page_directory[new_record.rid][i] = (tail_page_index, pos)
            self.bufferpool.mark_frame_used(frame_num)



    def __grabConsolidatedStack(self, base_rid):
        '''Given a base rid returns the consolidated page's rid and timestamp in a stack and the indireciton rid to latest tail page'''
        base_page_location = self.table.page_directory.get(base_rid, None)
        if base_page_location is None:
            raise ValueError("Record not found in Page Directory")
        
        indirection_rid = None 
        prev_rid = base_rid
        # stack to store the latest base/consolidated page's timestamp data
        base_pages_queue = []
        
        while (len(base_page_location) == 1 and indirection_rid != base_rid):
            base_page_number, base_page_index = base_page_location[0]

            indir_tuple = self.__getPageTuple(None, INDIRECTION_COLUMN, base_page_number)
            indir_frame, indir_frame_num = indir_tuple
            indirection_rid = indir_frame.page.get(base_page_index)
            self.bufferpool.mark_frame_used(indir_frame_num)


            ts_tuple = self.__getPageTuple(None, TIMESTAMP_COLUMN, base_page_number)
            ts_frame, ts_frame_num = ts_tuple
            timestamp = ts_frame.page.get(base_page_index)
            self.bufferpool.mark_frame_used(ts_frame_num)

            base_pages_queue.append((prev_rid, timestamp))

            prev_rid = indirection_rid
            base_page_location = self.table.page_directory.get(indirection_rid, None)

        return base_pages_queue, indirection_rid

    # Since we're moving to using tuples, it's easier to have a function that just gives the frame and frame num
    def __getPageTuple(self, page_range_index, record_column, page_index):

        frame = self.bufferpool.get_page_frame(page_range_index, record_column, page_index)
        if(frame is None):
            raise ValueError ("No tuple exists within given page range index")
        
        frame_num = self.bufferpool.get_page_frame_num(page_range_index, record_column, page_index)
        return (frame, frame_num)
