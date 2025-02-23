from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from lstore.page_range import PageRange
from lstore.config import *
from lstore.bufferpool import BufferPool, Frame


from time import time
from math import floor

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table:Table = table
        self.bufferpool:BufferPool = self.table.bufferpool
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    # Delete was simplified to just locate rid and put it on the queue, then deleting indices. Actual process will occur in merge function
    def delete(self, primary_key):

        # Locate the RID associated with the primary key
        base_rid = self.table.index.locate(self.table.key, primary_key)
        if(base_rid is None):
            return False  # Record does not exist

        self.table.diallocation_rid_queue.put(base_rid[0])
        self.table.index.delete_from_index(self.table.key, primary_key, base_rid[0])

        # Deletion successful
        return True

    

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):


        # print(f"Columns: {columns}")
        # print(f"Expected Index: {NUM_HIDDEN_COLUMNS + self.table.key}")
        # print(f"Columns Length: {len(columns)}")
        
        # columns[NUM_HIDDEN_COLUMNS + self.table.key] was giving an error for index out of range? Metadata not considered?
        if (self.table.index.locate(self.table.key, columns[self.table.key])):
            return False

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
        hidden_columns[BASE_PAGE_ID_COLUMN] = record.rid
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
            page_range_index, base_page_index, base_page_slot = self.table.get_base_record_location(rid)

            projected_columns_schema = 0
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 1:
                    projected_columns_schema |= (1 << i)

            # Since primary key is never updated we can grab the primary key column directly from the base page
            if (projected_columns_schema >> self.table.key) & 1 == 1:

                record_columns[self.table.key] = self.table.bufferpool.read_page_slot(page_range_index, NUM_HIDDEN_COLUMNS + self.table.key, base_page_index, base_page_slot)
                projected_columns_schema &= ~(1 << self.table.key)

            # store the (base) page# and index# that the RID/row is located
            consolidated_stack, indirection_rid = self.__grabConsolidatedStack(rid)
            
            if len(consolidated_stack) == 0:
                consolidated_rid = rid
                consolidated_timestamp = self.table.bufferpool.read_page_slot(page_range_index,TIMESTAMP_COLUMN, base_page_index, base_page_slot)

            else:
                consolidated_rid, consolidated_timestamp = consolidated_stack.pop()

            # print(f"RID: {rid}, Base RID: {consolidated_rid}, Timestamp: {consolidated_timestamp}, Indirection RID: {indirection_rid}")

            # traverse through the tail pages based on the relative version
            current_version = 0
            page_locations = self.table.page_ranges[page_range_index].logical_directory.get(indirection_rid, None)

            while indirection_rid != rid and current_version > relative_version and page_locations is not None:
                current_version -= 1
                indirection_rid = self.table.bufferpool.read_page_slot(page_range_index, INDIRECTION_COLUMN, page_locations[1])
                tail_timestamp = self.table.bufferpool.read_page_slot(page_range_index, TIMESTAMP_COLUMN, page_locations[1])

                # Skip over newer consolidated pages
                if consolidated_timestamp > tail_timestamp:
                    if len(consolidated_stack) > 0:
                        consolidated_rid, consolidated_timestamp = consolidated_stack.pop()

                    else:
                        consolidated_rid = rid
                        consolidated_timestamp = self.table.bufferpool.read_page_slot(page_range_index, TIMESTAMP_COLUMN, base_page_index, base_page_slot)

                page_locations = self.table.page_ranges[page_range_index].logical_directory.get(indirection_rid, None)

            #print("Consolidated stack", consolidated_stack)
            #print("page Locations", page_locations)

            if(page_locations is not None and len(page_locations) > 1):
                while projected_columns_schema > 0 and indirection_rid != rid and tail_timestamp >= consolidated_timestamp:

                    tail_timestamp = self.table.bufferpool.read_page_slot(page_range_index, TIMESTAMP_COLUMN, page_locations[1])
                    schema_column = self.table.bufferpool.read_page_slot(page_range_index, SCHEMA_ENCODING_COLUMN, page_locations[1])
                    
                    for i in range(self.table.num_columns):
                        if (((projected_columns_schema >> i) & 1 == 1) and ((schema_column >> i) & 1 == 1)):
                            
                            record_columns[i] = self.table.bufferpool.read_page_slot(page_range_index, NUM_HIDDEN_COLUMNS + i, page_locations[1])
                            projected_columns_schema &= ~(1 << i)
                            
                        if projected_columns_schema == 0:
                            break

                    indirection_rid = self.table.bufferpool.read_page_slot(page_range_index, INDIRECTION_COLUMN, page_locations[1])
                    page_locations = self.table.page_ranges[page_range_index].logical_directory.get(indirection_rid, None)
                    
                    if page_locations is not None:
                        tail_timestamp = self.table.bufferpool.read_page_slot(page_range_index, TIMESTAMP_COLUMN, page_locations[1])

                    else:
                        break

            if projected_columns_schema > 0:
                if consolidated_rid == rid:

                    _, consolidated_page_index, consolidated_page_slot = self.table.get_base_record_location(rid)

                else:
                    consolidated_page_index, consolidated_page_slot = self.table.page_ranges[page_range_index].get_column_location(consolidated_rid, NUM_HIDDEN_COLUMNS + self.table.key)
                
                for i in range(self.table.num_columns):
                    if (projected_columns_schema >> i) & 1 == 1:
                        record_columns[i] = self.table.bufferpool.read_page_slot(page_range_index, NUM_HIDDEN_COLUMNS + i, consolidated_page_index, consolidated_page_slot)
                        projected_columns_schema &= ~(1 << i)

            record_objs.append(Record(rid, record_columns[self.table.key], record_columns))
        return record_objs


    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking (Ignore this for now)
    """
    def update(self, primary_key, *columns):

        rid_location = self.table.index.locate(self.table.key, primary_key)
        if(rid_location is None):
            print("Update Error: Record does not exist")
            return False
        
        new_columns = [None] * self.table.total_num_columns

        schema_encoding = 0
        projected_columns = []


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

        page_range_index, page_index, page_slot = self.table.get_base_record_location(rid_location[0])
        _, prev_tail_rid = self.__grabConsolidatedStack(rid_location[0])

        new_columns[INDIRECTION_COLUMN] = prev_tail_rid
        new_columns[SCHEMA_ENCODING_COLUMN] = schema_encoding
        new_columns[TIMESTAMP_COLUMN] = int(time())

        new_record = Record(rid = -1, key = primary_key, columns = new_columns)
        self.table.assign_rid_to_record(new_record)
        self.table.page_ranges[page_range_index].write_tail_record(new_record.rid, *new_columns)
        self.table.bufferpool.write_page_slot(page_range_index, INDIRECTION_COLUMN, page_index, page_slot, new_record.rid)

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
            page_range_index, base_page_index, base_page_slot = self.table.get_base_record_location(rid)

            # store the (base) page# and index# that the RID/row is located
            consolidated_stack, indirection_rid = self.__grabConsolidatedStack(rid)
            if len(consolidated_stack) == 0:
                consolidated_rid = rid
                consolidated_timestamp = self.table.bufferpool.read_page_slot(page_range_index, TIMESTAMP_COLUMN, base_page_index, base_page_slot)
            else:
                consolidated_rid, consolidated_timestamp = consolidated_stack.pop()

            # print(f"RID: {rid}, Base RID: {consolidated_rid}, Timestamp: {consolidated_timestamp}, Indirection RID: {indirection_rid}")

            # traverse through the tail pages based on the relative version
            current_version = 0
            page_locations = self.table.page_ranges[page_range_index].logical_directory.get(indirection_rid, None)
            
            while(indirection_rid != rid and current_version > relative_version):
                current_version -= 1

                indirection_rid = self.table.bufferpool.read_page_slot(page_range_index, INDIRECTION_COLUMN, page_locations[1])
                tail_timestamp = self.table.bufferpool.read_page_slot(page_range_index, TIMESTAMP_COLUMN, page_locations[1])

                if(consolidated_timestamp > tail_timestamp):
                    if len(consolidated_stack) > 0:
                        consolidated_rid, consolidated_timestamp = consolidated_stack.pop()
                    else:
                        consolidated_rid = rid
                        consolidated_timestamp = self.table.bufferpool.read_page_slot(page_range_index, TIMESTAMP_COLUMN, base_page_index, base_page_slot)

                page_locations = self.table.page_ranges[page_range_index].logical_directory.get(indirection_rid, None)

            #print("Consolidated stack", consolidated_stack)
            #print("page Locations", page_locations)
            
            if page_locations is not None and len(page_locations) > 1:
                tail_timestamp = self.table.bufferpool.read_page_slot(
                page_range_index, TIMESTAMP_COLUMN, page_locations[1])

                while (indirection_rid != rid and tail_timestamp >= consolidated_timestamp and aggregate_column_value is None):

                    schema_column = self.table.bufferpool.read_page_slot(page_range_index, SCHEMA_ENCODING_COLUMN, page_locations[1])

                    if (((schema_column >> aggregate_column_index) & 1 == 1)):
                        aggregate_column_value = self.table.bufferpool.read_page_slot(page_range_index, NUM_HIDDEN_COLUMNS + aggregate_column_index, page_locations[1])

                    indirection_rid = self.table.bufferpool.read_page_slot(page_range_index, INDIRECTION_COLUMN, page_locations[1])
                    page_locations = self.table.page_ranges[page_range_index].logical_directory.get(indirection_rid, None)

                    if page_locations is not None:
                        tail_timestamp = self.table.bufferpool.read_page_slot(page_range_index, TIMESTAMP_COLUMN, page_locations[1])

            # if we were unsuccessful in finding an older version - this implies that indirection_rid == rid and we can
            # thus directly retrieve from the base/consolidated page

            if aggregate_column_value is None:
                if consolidated_rid == rid or consolidated_rid not in self.table.page_ranges[page_range_index].logical_directory:
                    _, consolidated_page_index, consolidated_page_slot = self.table.get_base_record_location(rid)
                
                else:
                    consolidated_page_index, consolidated_page_slot = self.table.page_ranges[page_range_index].get_column_location(consolidated_rid, NUM_HIDDEN_COLUMNS + aggregate_column_index)
                
                aggregate_column_value = self.table.bufferpool.read_page_slot(page_range_index, NUM_HIDDEN_COLUMNS + aggregate_column_index,consolidated_page_index, consolidated_page_slot)

            if(aggregate_column_value is None):
                aggregate_column_value = 0

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
    
    
    def __grabConsolidatedStack(self, base_rid):
        '''Given a base rid returns the consolidated page's rid and timestamp in a stack and the indireciton rid to latest tail page'''
        page_range_index, page_index, page_slot = self.table.get_base_record_location(base_rid)
        
        if page_range_index is None:
            raise ValueError("Record not found in Page Directory")
        
        indirection_rid = None 
        prev_rid = base_rid
        # stack to store the latest base/consolidated page's timestamp data
        base_pages_queue = []
        
        while(len(self.table.page_ranges[page_range_index].logical_directory.get(prev_rid, [])) == 1 and indirection_rid != base_rid):
            indirection_rid = self.table.bufferpool.read_page_slot(page_range_index, INDIRECTION_COLUMN, page_index, page_slot)
            timestamp = self.table.bufferpool.read_page_slot(page_range_index, TIMESTAMP_COLUMN, page_index, page_slot)
            base_pages_queue.append((prev_rid, timestamp))

            if(indirection_rid == base_rid or indirection_rid is None):
                break  # Stop if we've looped back to the base record

            prev_rid = indirection_rid
            page_index, page_slot = self.table.page_ranges[page_range_index].get_column_location(indirection_rid, INDIRECTION_COLUMN)
        
        return base_pages_queue, indirection_rid
