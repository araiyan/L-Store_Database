from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from lstore.config import *

from time import time


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table:Table = table
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
        
        # Try to delete the element associated with rid from all indices. Return false if can't
        try:
            self.table.index.delete_from_all_indices(primary_key)
    
        except ValueError:
            return False

        # Start from the base record, we iterate through every indirection column 
        base_page_index, base_page_slot = self.table.page_directory[base_rid][0]
        # Record marked for deletion, we write the deletion flag to the base page
        self.table.base_pages[RID_COLUMN][base_page_index].write_precise(base_page_slot, RECORD_DELETION_FLAG)
        indirection_rid = self.table.base_pages[INDIRECTION_COLUMN][base_page_index].get(base_page_slot)
        column_page_locations = self.table.page_directory[indirection_rid]

        self.table.diallocation_rid_queue.put(base_rid)

        # As long as we don't interate back to the base record or the indirection points to None, we have records we need to go through
        while(indirection_rid != base_rid or indirection_rid is not None):

            # Add the indirection rid to the list to delete
            self.table.diallocation_rid_queue.put(indirection_rid)

            page_index, page_slot = column_page_locations[INDIRECTION_COLUMN]

            # Get the next indirection rid
            indirection_rid = self.table.base_pages[INDIRECTION_COLUMN][page_index].get(page_slot)
            column_page_locations = self.table.page_directory[indirection_rid]


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
                record_primary_key = self.table.base_pages[NUM_HIDDEN_COLUMNS + self.table.key][base_page_number].get(base_page_index)
                record_columns[self.table.key] = record_primary_key
                projected_columns_schema &= ~(1 << self.table.key)
            
            consolidated_stack, indirection_rid = self.__grab_consolidated_stack(rid)
            # store the (base) page# and index# that the RID/row is located

            consolidated_rid, consolidated_timestamp = consolidated_stack.pop()

            # print(f"RID: {rid}, Base RID: {consolidated_rid}, Timestamp: {consolidated_timestamp}, Indirection RID: {indirection_rid}")

            # traverse through the tail pages based on the relative version
            current_version = 0
            page_locations = self.table.page_directory[indirection_rid]
            while (indirection_rid != rid and current_version > relative_version):
                current_version -= 1
                indirection_rid = self.table.grab_tail_value_from_page_location(page_locations, INDIRECTION_COLUMN)
                tail_timestamp = self.table.grab_tail_value_from_page_location(page_locations, TIMESTAMP_COLUMN)

                # Skip over newer consolidated pages
                if (consolidated_timestamp > tail_timestamp):
                    consolidated_rid, consolidated_timestamp = consolidated_stack.pop()

                page_locations = self.table.page_directory[indirection_rid]

            #print("Consolidated stack", consolidated_stack)
            #print("page Locations", page_locations)
            if (len(page_locations) > 1):

                tail_timestamp = self.table.grab_tail_value_from_page_location(page_locations, TIMESTAMP_COLUMN)

                while (projected_columns_schema > 0 and indirection_rid != rid and tail_timestamp >= consolidated_timestamp):
                    tail_timestamp = self.table.grab_tail_value_from_page_location(page_locations, TIMESTAMP_COLUMN)
                    schema_column = self.table.grab_tail_value_from_page_location(page_locations, SCHEMA_ENCODING_COLUMN)

                    for i in range(self.table.num_columns):
                        if (((projected_columns_schema >> i) & 1 == 1) and ((schema_column >> i) & 1 == 1)):
                            record_columns[i] = self.table.grab_tail_value_from_page_location(page_locations, NUM_HIDDEN_COLUMNS + i)
                            projected_columns_schema &= ~(1 << i)

                        if projected_columns_schema == 0:
                            break

                    indirection_rid = self.table.grab_tail_value_from_page_location(page_locations, INDIRECTION_COLUMN)
                    page_locations = self.table.page_directory[indirection_rid]

            # if we were unsuccessful in finding an older version - this implies that indirection_rid == rid and we can
            # thus directly retrieve from the base/consolidated page

            consolidated_page_location = self.table.page_directory.get(consolidated_rid, None)
            consolidated_page_index, consolidated_page_slot = consolidated_page_location[0]
            for i in range(self.table.num_columns):
                if (projected_columns_schema >> i) & 1 == 1:
                    record_columns[i] = self.table.base_pages[NUM_HIDDEN_COLUMNS + i][consolidated_page_index].get(consolidated_page_slot)
                    projected_columns_schema &= ~(1 << i) 
                
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
        prev_tail_rid = self.table.base_pages[INDIRECTION_COLUMN][cons_page_index].get(cons_page_slot)
        
        new_columns[INDIRECTION_COLUMN] = prev_tail_rid
        new_columns[SCHEMA_ENCODING_COLUMN] = schema_encoding
        new_columns[TIMESTAMP_COLUMN] = int(time())


        # Create new record and initialize it into the pd
        new_record = Record(rid = -1, key = primary_key, columns = new_columns)
        self.table.assign_rid_to_record(new_record)
        self.table.page_directory[new_record.rid] = [None] * self.table.total_num_columns
        new_columns[RID_COLUMN] =  new_record.rid
        self.__writeTailRecord(new_record)


        # Updates the indirection column on the base/cons page for new tail page
        page_index, page_slot = self.table.page_directory[cons_rid][0]
        self.table.base_pages[INDIRECTION_COLUMN][page_index].write_precise(page_slot, new_record.rid)        

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
        # Grabs all records rid
        # TODO: If each indicy is a binary tree we need to iterate through the binary tree and grab all rids
       
        # Get all record RIDs in the range using the **primary key index**        
        all_rids = self.table.index.locate_range(start_range, end_range, self.table.key)
       
       
        if not all_rids:
            return False  # Return False if no records exist in range
 
        total_sum = 0  # Initialize sum
       
        for base_rid in all_rids:
            print(base_rid)
            # Get base record location
            if base_rid not in self.table.page_directory:
                continue  # Skip if RID not found
 
            page_index, page_slot = self.table.page_directory[base_rid]
 
            # Check the Indirection Column for the latest version
            indirection = self.get_column_value(page_index, page_slot, self.table.indirection_column_index)
 
            latest_rid = base_rid  # Assume base_rid is latest unless indirection points to a newer RID
            if indirection and indirection != 0:
                latest_rid = indirection  # Follow latest RID
 
            # Get the latest column value
            if latest_rid not in self.table.page_directory:
                continue  # Skip if no valid latest record found
 
            latest_page_index, latest_page_slot = self.table.page_directory[latest_rid]
            latest_value = self.get_column_value(latest_page_index, latest_page_slot, aggregate_column_index)
 
            total_sum += latest_value  # Accumulate the sum
 
        return total_sum if total_sum != 0 else False

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
       # records_list = self.table.index.locate_range(start_range, end_range, self.table.key)
       # sum_total = 0
       # for rid in records_list:
       #     pass
        
       # return sum_total
       
       # Get all record RIDs in the range using the **primary key index**        
        all_rids = self.table.index.locate_range(start_range, end_range, self.table.key)
       
        if not all_rids:
            return False  # Return False if no records exist in range
 
        total_sum = 0  # Initialize sum
 
        for base_rid in all_rids:
            # Get base record location
            if base_rid not in self.table.page_directory:
                continue  # Skip if RID not found
 
            page_index, page_slot = self.table.page_directory[base_rid]
 
            # Follow the version chain based on `relative_version`
            current_rid = base_rid
            for _ in range(relative_version):
                indirection = self.get_column_value(page_index, page_slot, self.table.indirection_column_index)
 
                if indirection is None or indirection == 0:
                    break  # Stop if no further versions exist
 
                current_rid = indirection
                if current_rid not in self.table.page_directory:
                    break  # Stop if no valid RID found
 
                page_index, page_slot = self.table.page_directory[current_rid]
 
            # Get the value at the requested version
            latest_value = self.get_column_value(page_index, page_slot, aggregate_column_index)
            total_sum += latest_value  # Accumulate the sum
 
 
        return total_sum if total_sum != 0 else False
 
    """
    Increments one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def get_column_value(self, page_index, page_slot, column_index):
   
        return self.table.pages[page_index][column_index][page_slot]  # Retrieve value from in-memory storage
    
    """
    Retrieves a column value from a page.
    Assumes the table has a dictionary `pages` storing data.
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

        # Assuming that there are none, we want to set the indirection rid to be the base page's; this would point to the latest tail
        indirection_rid = self.table.base_pages[INDIRECTION_COLUMN][base_page_index].get(base_page_slot)

        # If no cons; newest is base
        previous_rid = base_rid

        ## This is moreso just for checking tuple length to ensure we're looking at a base/consolidated page
        column_page_locations = self.table.page_directory[indirection_rid]

        # If there are consolidated pages w/ we walk them for indirection ptr that points to the latest tail
        while(indirection_rid != base_rid and len(column_page_locations) == 1):
            
            previous_rid = indirection_rid
            page_index, page_slot = column_page_locations[0]
            indirection_rid = self.table.base_pages[INDIRECTION_COLUMN][page_index].get(page_slot)
            column_page_locations = self.table.page_directory[indirection_rid]
        
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

            # Current page is the latest tail page
            curr_page: Page = self.table.tail_pages[i][-1]
            
            # Writing to current page, we get the position of the tail record in the page & index of the page
            pos = curr_page.write(new_record.columns[i])
            tail_page_index = len(self.table.tail_pages[i]) - 1

            # Write this to the pd
            self.table.page_directory[new_record.rid][i] = (tail_page_index, pos)

    def __grab_consolidated_stack(self, base_rid):
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
            indirection_rid = self.table.base_pages[INDIRECTION_COLUMN][base_page_number].get(base_page_index)
            timestamp = self.table.base_pages[TIMESTAMP_COLUMN][base_page_number].get(base_page_index)
            base_pages_queue.append((prev_rid, timestamp))

            prev_rid = indirection_rid
            base_page_location = self.table.page_directory.get(indirection_rid, None)

        return base_pages_queue, indirection_rid
