import sys
sys.path.append("..")

from lstore.page_range import PageRange
from lstore.bufferpool import BufferPool
from lstore.table import Table
from lstore.config import NUM_HIDDEN_COLUMNS

import json

from random import choice, randint, sample, seed
import unittest
import shutil


class TestPageRange(unittest.TestCase):
  
    def __init__(self, methodName = "runTest"):
        super().__init__(methodName)
        self.num_records = 24000
        self.num_columns = 5
        self.table:Table = Table("PageRangeTestTable", self.num_columns, 0, "TestDB")
        self.bufferpool = BufferPool(self.table.table_path)
        self.records = {}

        for i in range(self.num_records):
            self.records[i] = [randint(0, 100) for _ in range(self.num_columns)]

        self.updated_records = {}
        self.update_tail_records = {}
        for key in self.records:
            updated_columns = [None, None, None, None, None]
            self.updated_records[key] = self.records[key].copy()
            for i in range(randint(0, 3), randint(3, self.table.num_columns)):
                # updated value
                value = randint(0, 20)
                updated_columns[i] = value
                # update our test directory
                self.updated_records[key][i] = value
            
            self.update_tail_records[key] = updated_columns


    def test_write_base_record(self):
        page_range = PageRange(0, self.num_columns, self.bufferpool)

        # instantiate page
        for i in range(self.num_records):
            #print(f"Writing Record to PageRange: {self.records[i]}")
            _, page_index, page_slot = self.table.get_base_record_location(i)
            page_range.write_base_record(page_index, page_slot, *self.records[i])

        # ensure tail page indexes are updated
        self.assertNotEqual(page_range.tail_page_index, [0] * self.num_columns)
        # logical directory should not be updated on base record write
        self.assertEqual(page_range.logical_directory, {})

        print("Test Page Range Base Record Write Passed")

        self.bufferpool.unload_all_frames()
        shutil.rmtree(self.table.db_path)

    def test_write_tail_record(self):
        page_range = PageRange(1, self.num_columns, self.bufferpool)
        # instantiate page
        for i in range(self.num_records):
            #print(f"Writing Record to PageRange: {self.records[i]}")
            logical_rid = page_range.assign_logical_rid()
            hidden_columns = [0] * NUM_HIDDEN_COLUMNS
            all_columns = hidden_columns + self.update_tail_records[i]
            page_range.write_tail_record(logical_rid, *all_columns)

        # ensure tail page indexes are updated
        self.assertNotEqual(page_range.tail_page_index, [0] * self.num_columns)
        # logical directory should be updated on tail record write
        self.assertNotEqual(page_range.logical_directory, {})

        for i in range(self.table.num_columns):
            if (page_range.logical_directory[page_range.logical_rid_index - 1][i] != None):
                self.assertLess(page_range.logical_directory[page_range.logical_rid_index - 1][i], page_range.logical_rid_index)

        print("Test Page Range Tail Record Write Passed")

        self.bufferpool.unload_all_frames()
        shutil.rmtree(self.table.db_path)

    def test_serialize(self):
        page_range = PageRange(2, self.num_columns, self.bufferpool)
        # instantiate page
        for i in range(self.num_records):
            #print(f"Writing Record to PageRange: {self.records[i]}")
            logical_rid = page_range.assign_logical_rid()
            hidden_columns = [0] * NUM_HIDDEN_COLUMNS
            all_columns = hidden_columns + self.update_tail_records[i]
            page_range.write_tail_record(logical_rid, *all_columns)

        self.bufferpool.unload_all_frames()

        # serialize page range
        with open(f"{self.table.table_path}/page_range_3.bin", "w", encoding="utf-8") as file:
            json.dump(page_range.serialize(), file)

        new_page_range = PageRange(3, self.num_columns, self.bufferpool)
        with open(f"{self.table.table_path}/page_range_3.bin", "r", encoding="utf-8") as file:
            page_range_data = json.load(file)
            new_page_range.deserialize(page_range_data)

        self.maxDiff = None
        self.assertEqual(page_range.logical_directory, new_page_range.logical_directory)
        self.assertEqual(page_range.tail_page_index, new_page_range.tail_page_index)
        self.assertEqual(page_range.logical_rid_index, new_page_range.logical_rid_index)
        self.assertEqual(page_range.tps, new_page_range.tps)

        print("Test Page Range Serialize Passed")

        self.bufferpool.unload_all_frames()
        shutil.rmtree(self.table.db_path)



if __name__ == '__main__':
  print("Running Page Range Test")
  unittest.main()
