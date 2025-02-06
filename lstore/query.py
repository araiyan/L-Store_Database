from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from lstore.config import NUM_HIDDEN_COLUMNS
from lstore.config import INDIRECTION_COLUMN



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
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        # Note: Ensure schema_encoding is converted to int before inserted into record.column
        pass

    
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
        pass

    
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
        pass

    
        """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):

        # We want to see if the record exists with the specified key; no point in continuing further without checking
        rid_location = self.table.index.locate(self.table.key, primary_key)
        if(rid_location is None or len(columns) != self.table.num_columns):
            return False

        # The tail should have the same number of columns as the other pages so we should be multiplying by the total_num_columns value
        # Multiplying by [None] atm since the columns don't have an assigned size
        new_columns = [None] * self.table.total_num_columns

        # Insert values into the new column
        for i, value in enumerate(columns):

            # If we're modifying the primary_key then this update should be stopped since we can't change the primary_key column
            if(i == self.table.key and value != primary_key):
                return False
            
            new_columns[NUM_HIDDEN_COLUMNS + i] = value

        # Create new record and initialize it into the pd
        new_record = Record(rid = -1, key = primary_key, columns = new_columns)
        self.table.assign_rid_to_record(new_record)
        self.table.page_directory[new_record.rid] = [None] * self.table.total_num_columns
        self.__writeTailRecord(new_record)

        # Updates the indirection column on the base page for new tail page
        page_index, page_slot = self.table.page_directory[rid_location]
        self.table.base_pages[page_index].write_precise(page_slot, new_record.rid)

        # Update indirection ptr chain & indices
        self.__updateIndirectionChain(rid_location, new_record.rid)
        self.table.index.update_all_indices(rid_location, columns)

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
    


    """Helper function that writes the new record into the appropriate tail pages."""
    def __writeTailRecord(self, new_record):

        for i in range(self.table.total_num_columns):
            
            # Skip columns we're not updating
            if(new_record.columns[i] is None):
                continue

            # If current tail page is full, append a new page
            if(not self.table.tail_pages[i][-1].has_capacity()):
                self.table.tail_pages[i].append(Page())

            curr_page: Page = self.table.tail_pages[i][-1]
            curr_page.write(new_record.columns[i])
            page_index = curr_page.num_records  

            # Update page directory for the new record
            # TODO: Kind of a hack way of doing it. Need to improve indexing updated columns once pages is solidified
            self.table.page_directory[new_record.rid][i] = [len(self.table.tail_pages[i]) - 1, page_index]
    


    """Helper function to update the indirection pointer chain for an update."""
    def __updateIndirectionChain(self, base_rid, new_rid):
    
        base_page_index, base_page_slot = self.table.page_directory[base_rid][0]
        current_indirection = self.table.base_pages[INDIRECTION_COLUMN][base_page_index].get(base_page_slot)

        # If no prior update, update the base record's pointer
        if(current_indirection == base_rid):
            self.table.base_pages[INDIRECTION_COLUMN][base_page_index].write_precise(base_page_slot, new_rid)
        
        # Walk the chain until the end or until the condition is met
        else:
            cons_rid = current_indirection
            
            
            while(True):
                cons_page_index, cons_page_slot = self.table.page_directory[cons_rid][0]
                next_indirection = self.table.base_pages[INDIRECTION_COLUMN][cons_page_index].get(cons_page_slot)
                
                if(next_indirection == base_rid or next_indirection is None):
                    break
                
                cons_rid = next_indirection
            
            cons_page_index, cons_page_slot = self.table.page_directory[cons_rid][0]
            self.table.base_pages[INDIRECTION_COLUMN][cons_page_index].write_precise(cons_page_slot, new_rid)

    """
    Given the base record's RID and a column number,
    traverse indirection chain to find last updated value
    for that column
        
    Returns the latest value found. If no tail record updated this column,
    returns the base record's value
    """
    def __getLatestColumnValue(self, rid_location, col):
        
        # I think my mistake here was that I originally deleted the base record call for indirection column so I was just doing self.table.page_directory[rid_location][INDIRECITON_COLUMN]?
        # Would col be an issue here if the update() function checks if the column is None and skips if it is?
        
        base_page_index, base_page_slot = self.table.page_directory[rid_location][col]
        indirection_rid = self.table.base_pages[INDIRECTION_COLUMN][base_page_index].get(base_page_slot)
        pd_entry = self.table.page_directory[indirection_rid]

                
        # Walk the update chain using a loop that exits once we find the latest non-consolidated record and we're not the base record
        while(indirection_rid != rid_location and len(pd_entry) == 1):
            
            rec_page_index, rec_page_slot = pd_entry[0]
            indirection_rid = self.table.base_pages[INDIRECTION_COLUMN][rec_page_index].get(rec_page_slot)
            pd_entry = self.table.page_directory[indirection_rid]
                

        # Use location stored for the specific column and return
        record_loc = pd_entry[col]
        rec_page_index, rec_page_slot = record_loc
        return self.table.tail_pages[col][rec_page_index].get(rec_page_slot)
