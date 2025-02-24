import sys
sys.path.append("..")

from lstore.page_range import PageRange
from lstore.bufferpool import BufferPool
from lstore.table import Table
from lstore.config import *

import json
import queue

from random import choice, randint, sample, seed
import unittest
import shutil


class TestTableSerialize(unittest.TestCase):

    def test_table_write_serialize(self):
        num_records = 24000
        num_columns = 5
        table = Table("PageRangeTestTable", num_columns, 0, "TestDB")
        records = {}

        for i in range(num_records):
            records[i] = [randint(0, 100) for _ in range(num_columns)]

        # instantiate page
        page_range_index = 0
        table.page_ranges.append(PageRange(page_range_index, table.num_columns, table.bufferpool, table.merge_queue))

        for i in range(num_records):
            #print(f"Writing Record to PageRange: {records[i]}")
            if (i % MAX_RECORD_PER_PAGE_RANGE == 0):
                page_range_index += 1
                table.page_ranges.append(PageRange(page_range_index, table.num_columns, table.bufferpool, table.merge_queue))

            _, page_index, page_slot = table.get_base_record_location(i)
            table.page_ranges[page_range_index].write_base_record(page_index, page_slot, records[i])
        
        table.bufferpool.unload_all_frames()

         # serialize table
        with open(f"{table.table_path}/page_range_3.bin", "w", encoding="utf-8") as file:
            json.dump(table.serialize(), file)

        new_table:Table = Table(table.name, table.num_columns, table.key, table.db_path)

        with open(f"{new_table.table_path}/page_range_3.bin", "r", encoding="utf-8") as file:
            table_data = json.load(file)
            new_table.deserialize(table_data)

        self.assertEqual(table.name, new_table.name)
        self.assertEqual(table.num_columns, new_table.num_columns)
        self.assertEqual(table.key, new_table.key)
        self.assertEqual(table.db_path, new_table.db_path)
        self.assertEqual(table.rid_index, new_table.rid_index)
        self.assertEqual(table.page_directory, new_table.page_directory)
        self.assertEqual(table.page_ranges, new_table.page_ranges)

        for i in range(len(table.page_ranges)):
            self.assertEqual(table.page_ranges[i].logical_directory, new_table.page_ranges[i].logical_directory)
            self.assertEqual(table.page_ranges[i].tail_page_index, new_table.page_ranges[i].tail_page_index)
            self.assertEqual(table.page_ranges[i].page_range_index, new_table.page_ranges[i].page_range_index)

        print("Test Table Serialize Passed")

        new_table.bufferpool.unload_all_frames()
        shutil.rmtree(table.db_path)

    def test_table_update_serialize(self):
        num_records = 24000
        num_columns = 5
        table = Table("PageRangeTestTable", num_columns, 0, "TestDB")
        records = {}

        for i in range(num_records):
            records[i] = [randint(0, 100) for _ in range(num_columns)]

        updated_records = {}
        update_tail_records = {}
        for key in records:
            updated_columns = [None, None, None, None, None]
            updated_records[key] = records[key].copy()
            for i in range(randint(0, 3), randint(3, table.num_columns)):
                # updated value
                value = randint(0, 20)
                updated_columns[i] = value
                # update our test directory
                updated_records[key][i] = value
            
            update_tail_records[key] = updated_columns

        # instantiate page
        page_range_index = 0
        table.page_ranges.append(PageRange(page_range_index, table.num_columns, table.bufferpool, table.merge_queue))

        for i in range(num_records):
            #print(f"Writing Record to PageRange: {records[i]}")
            if (i % MAX_RECORD_PER_PAGE_RANGE == 0):
                page_range_index += 1
                table.page_ranges.append(PageRange(page_range_index, table.num_columns, table.bufferpool, table.merge_queue))

            _, page_index, page_slot = table.get_base_record_location(i)
            table.page_ranges[page_range_index].write_base_record(page_index, page_slot, records[i])
        
        table.bufferpool.unload_all_frames()

        page_range_index = 0
        for i in range(len(updated_records)):
            #print(f"Writing Record to PageRange: {self.records[i]}")
            if (i % MAX_RECORD_PER_PAGE_RANGE == 0):
                page_range_index += 1

            logical_rid = table.page_ranges[page_range_index].assign_logical_rid()
            hidden_columns = [0] * NUM_HIDDEN_COLUMNS
            all_columns = hidden_columns + update_tail_records[i]
            table.page_ranges[page_range_index].write_tail_record(logical_rid, *all_columns)

         # serialize table
        with open(f"{table.table_path}/page_range_3.bin", "w", encoding="utf-8") as file:
            json.dump(table.serialize(), file)

        new_table:Table = Table(table.name, table.num_columns, table.key, table.db_path)

        with open(f"{new_table.table_path}/page_range_3.bin", "r", encoding="utf-8") as file:
            table_data = json.load(file)
            new_table.deserialize(table_data)

        self.assertEqual(table.name, new_table.name)
        self.assertEqual(table.num_columns, new_table.num_columns)
        self.assertEqual(table.key, new_table.key)
        self.assertEqual(table.db_path, new_table.db_path)
        self.assertEqual(table.rid_index, new_table.rid_index)
        self.assertEqual(table.page_directory, new_table.page_directory)
        self.assertEqual(table.page_ranges, new_table.page_ranges)

        for i in range(len(table.page_ranges)):
            self.assertEqual(table.page_ranges[i].logical_directory, new_table.page_ranges[i].logical_directory)
            self.assertEqual(table.page_ranges[i].tail_page_index, new_table.page_ranges[i].tail_page_index)
            self.assertEqual(table.page_ranges[i].page_range_index, new_table.page_ranges[i].page_range_index)

        print("Test Table Serialize Passed")

        new_table.bufferpool.unload_all_frames()
        shutil.rmtree(table.db_path)

if __name__ == '__main__':
  print("Running Page Range Test")
  unittest.main()
