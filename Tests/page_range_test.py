import sys
sys.path.append("..")

from lstore.page_range import PageRange
from lstore.bufferpool import BufferPool
from lstore.table import Table
from lstore.config import NUM_HIDDEN_COLUMNS

from random import choice, randint, sample, seed
import unittest


class TestPageRange(unittest.TestCase):
  
    def __init__(self, methodName = "runTest"):
        super().__init__(methodName)
        self.num_records = 24
        self.num_columns = 3
        self.table = Table("PageRangeTestTable", self.num_columns, 0, "TestDB")
        self.bufferpool = BufferPool(self.table.table_path)
        self.page_range = PageRange(0, self.num_columns, self.bufferpool)
        self.records = {}

        for i in range(self.num_records):
            self.records[i] = [randint(0, 100) for _ in range(self.num_columns)]
            print(f"Created Record: {self.records[i]}")

        print("\n")

    def test_write_base_record(self):
        # instantiate page
        for i in range(self.num_records):
            #print(f"Writing Record to PageRange: {self.records[i]}")
            _, page_index, page_slot = self.table.get_base_record_location(i)
            self.page_range.write_base_record(page_index, page_slot, *self.records[i])

        # ensure tail page indexes are updated
        self.assertNotEqual(self.page_range.tail_page_index, [0] * self.num_columns)
        # logical directory should not be updated on base record write
        self.assertEqual(self.page_range.logical_directory, {})

        print("Test Page Range Base Record Write Passed")

    def test_write_tail_record(self):
        # instantiate page
        for i in range(self.num_records):
            #print(f"Writing Record to PageRange: {self.records[i]}")
            logical_rid = self.page_range.assign_logical_rid()
            self.page_range.write_tail_record(logical_rid, *self.records[i])

        # ensure tail page indexes are updated
        self.assertNotEqual(self.page_range.tail_page_index, [0] * self.num_columns)
        # logical directory should be updated on tail record write
        self.assertNotEqual(self.page_range.logical_directory, {})

        print("Test Page Range Tail Record Write Passed")



if __name__ == '__main__':
  print("Running Page Range Test")
  unittest.main()
