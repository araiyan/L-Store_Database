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
        try:
            # retrieve a list of RIDs that contain the "search_key" value within the column as defined by "search_key_index"
            rid_list = self.table.index.locate(search_key_index, search_key)

            # if there exists no RIDs that match the given parameters, return False
            if not rid_list:
                return False

            record_objs = []

            # iterate through all desired RIDs
            for rid in rid_list:
                base_page_location = self.table.page_directory.get(rid, None)
                if base_page_location is None:
                    continue
                    
                # store the (base) page# and index# that the RID/row is located
                base_page_number, base_page_index = base_page_location[0]

                # store base values within projected columns - we will update values with latest data as necessary 
                record_columns = [
                    self.table.base_pages[NUM_HIDDEN_COLUMNS + i][base_page_number].get(base_page_index)
                    for i in range(len(projected_columns_index)) if projected_columns_index[i] == 1
                ]

                # retrieve indirection RID so that we can traverse through updated pages/records
                indirection_rid = self.table.base_pages[INDIRECTION_COLUMN][base_page_number].get(base_page_index)

                while indirection_rid != rid:
                    # grab schema encoding value to determine whether or not a given column within a tail record was updated
                    schema_encoding = self.table.__grab_tail_value_from_rid(indirection_rid, SCHEMA_ENCODING_COLUMN)
                    
                    for i in range(len(projected_columns_index)):
                        if projected_columns_index[i] == 1 and ((schema_encoding >> i) & 1) == 1:
                            record_columns[i] = self.table.__grab_tail_value_from_rid(indirection_rid, NUM_HIDDEN_COLUMNS + i) 
                            projected_columns_index[i] = 0 # we no longer want to update this column - otherwise, we would be overriding newer data with old data

                    # update indirection_rid to the next (previous version) within chain of tail records
                    indirection_rid = self.table.__grab_tail_value_from_rid(indirection_rid, INDIRECTION_COLUMN)

                record_objs.append(Record(rid, search_key, record_columns))

            return record_objs if record_objs else False
        
        except Exception as e:
            print(f"Error in select: {e}")
            return False

    
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
        try:
            rid_list = self.table.index.locate(search_key_index, search_key)

            if not rid_list:
                raise ValueError("No records found with the given key")
            
            record_objs = []

            for rid in rid_list:
                base_page_location = self.table.page_directory.get(rid, None)
                if base_page_location is None:
                    continue

                base_page_number, base_page_index = base_page_location[0]

                indirection_rid = self.table.base_pages[INDIRECTION_COLUMN][base_page_number].get(base_page_index)

                # traverse through the tail pages based on the relative version
                current_version = 0
                while current_version > relative_version and indirection_rid != rid:
                    current_version -= 1
                    indirection_rid = self.table.__grab_tail_value_from_rid(indirection_rid, INDIRECTION_COLUMN)
                
                # unsuccessful in finding an older version - this implies that indirection_rid == rid and we can
                # thus directly retrieve from the base page
                if current_version > relative_version:
                    record_columns = [
                    self.table.base_pages[NUM_HIDDEN_COLUMNS + i][base_page_number].get(base_page_index)
                    for i in range(len(projected_columns_index)) if projected_columns_index[i] == 1
                ]
                # otherwise, retrieve from the relative version
                else:
                    record_columns = [
                    self.table.__grab_tail_value_from_rid(indirection_rid, NUM_HIDDEN_COLUMNS + i)
                    if projected_columns_index[i] == 1 else None
                    for i in range(len(projected_columns_index))
                ]
                    
                record_objs.append(Record(rid, search_key, record_columns))
                
            return record_objs if record_objs else False
        
        except Exception as e:
            print(f"Error during select_version: {e}")
            return False

    
        """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking (Ignore this for now)
    """
    def update(self, primary_key, *columns):

        # We want to see if the record exists with the specified key; no point in continuing further without checking
        rid_location = self.table.index.locate(self.table.key, primary_key)
        if(rid_location is None):
            return False
        
        # The tail should have the same number of columns as the other pages so we should be multiplying by the total_num_columns value
        # Multiplying by [None] atm since the columns don't have an assigned size
        new_columns = [None] * self.table.total_num_columns

        schema_encoding = 0
        projected_columns = []

        # Insert values into the new column
        for i, value in enumerate(columns):

            # If we're modifying the primary_key then this update should be stopped since we can't change the primary_key column
            if(i == self.table.key or value == primary_key):
                return False
            
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

        latest_record = self.select(primary_key, self.table.key, projected_columns)

        # Update indices
        self.table.index.update_all_indices(primary_key, *new_columns)

        # Update successful
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
        pass

    
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
        pass

    
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
