import os
import json
from lstore.table import Table
from lstore.bufferpool import Bufferpool
from lstore.index import Index

class Database():

    def __init__(self):
        self.tables:dict = {}
        self.path = None
        self.bufferpool = None
        pass

    def open(self, path):
        """Loads database from disk and restores tables, indexes, and pages"""
        self.path = path
        self.bufferpool = Bufferpool(path)

        # create new path if database path doesn't exist
        if not os.path.exists(path):
            os.makedirs(path)
        
        # grab table metadata - we assume that input path contains subpath to tables.json
        tables_metadata_path = os.path.join(path, "tables.json")
        if os.path.exists(tables_metadata_path):
            with open(tables_metadata_path, "r") as file:
                tables_metadata = json.load(file)

                # loops through tables and adds them to the self.tables dictionary
                for table_name, table_info in tables_metadata.items():
                    table = Table(table_name, table_info["num_columns"], table_info["key_index"])
                    self.tables[table_name] = table

                    # restores page directory for table
                    table.page_directory = table_info["page_directory"]

                    # initialize indexes for table
                    table.index = Index(table)
                    for column_idx, indexed_values in table_info["indexes"].items():
                        table.index.create_index(int(column_idx))

                        # loop through inner dictionary and load stored values into the created index
                        for value, rids in indexed_values.items():
                            for rid in rids:
                                table.index.insert_to_index(int(column_idx, value, rid))

                    # load pages from disk
                    table_path = os.path.join(path, table_name)
                    if os.path.exists(table_path):
                        for page_range in os.listdir(table_path):
                            page_range_path = os.path.join(table_path, page_range)
                            if os.path.isdir(page_range_path) and page_range.startswith("PageRange_"):

                                # grab pange range index from path name
                                page_range_index = int(page_range.split("_")[1])

                                for file in os.listdir(page_range_path):
                                    if file.startswith("Page_") and file.endswith(".bin"):
                                        parts = file.split("_")
                                        column_index = int(parts[1])
                                        page_index = int(parts[2].split(".")[0])

                                        # load page into bufferpool
                                        page, frame_num = self.bufferpool.get(page_range_index, column_index, page_index)
                                        if page is not None:
                                            print(f"Loaded {file} into bufferpool for table {table_name}")

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if self.tables.get(name) is not None:
            raise NameError(f"Error creating Table! Following table already exists: {name}")
        
        self.tables[name] = Table(name, num_columns, key_index)
        return self.tables[name]

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if self.tables.get(name) is None:
            raise NameError(f"Error dropping Table! Following table does not exist: {name}")
        
        del self.tables[name]

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if self.tables.get(name) is None:
            raise NameError(f"Error getting Table! Following table does not exist: {name}")
        
        return self.tables[name]
