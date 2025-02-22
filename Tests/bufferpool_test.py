import sys
sys.path.append("..")

from lstore.bufferpool import BufferPool
from lstore.table import Table
from lstore.config import NUM_HIDDEN_COLUMNS

from random import choice, randint, sample, seed
import unittest
import shutil


class TestBufferpool(unittest.TestCase):
  
    def __init__(self, methodName = "runTest"):
        super().__init__(methodName)
        self.num_records = 240
        self.num_columns = 12
        self.table = Table("PageRangeTestTable", self.num_columns, 0, "TestDB")
        self.records = {}

        for i in range(self.num_records):
            self.records[i] = [randint(0, 100) for _ in range(self.num_columns)]
            print(f"Created Record: {self.records[i]}")

        print("\n")

    def test_bufferpool_write(self):
        print("\nTesting Bufferpool Write")
        bufferpool = BufferPool(self.table.table_path)

        # instantiate page
        for i in range(self.num_records):
            for (j, column) in enumerate(self.records[i]):
                bufferpool.write_page_next(0, j, 0, column)

        for i in range(self.num_records):
            for (j, column) in enumerate(self.records[i]):
                page_slot_value = bufferpool.read_page_slot(0, j, 0, i)
                frame_num = bufferpool.get_page_frame_num(0, j, 0)
                bufferpool.mark_frame_used(frame_num)
                self.assertEqual(page_slot_value, column)

        print("Test bufferpool Write passed")

        # Deleting the bufferpool db
        shutil.rmtree(self.table.db_path)

    def test_bufferpool_unload(self):

        print("\nTesting bufferpool Unload")
        bufferpool = BufferPool(self.table.table_path)

        # instantiate page
        for i in range(self.num_records):
            for (j, column) in enumerate(self.records[i]):
                bufferpool.write_page_slot(0, j, 0, i, column)

        bufferpool.unload_all_frames()

        for i in range(self.num_records):
            for (j, column) in enumerate(self.records[i]):
                page_slot_value = bufferpool.read_page_slot(0, j, 0, i)
                frame_num = bufferpool.get_page_frame_num(0, j, 0)
                bufferpool.mark_frame_used(frame_num)
                self.assertEqual(page_slot_value, column)

        print("Test bufferpool Unload passed")

        # Deleting the bufferpool db
        shutil.rmtree(self.table.db_path)


if __name__ == '__main__':
    print("Running Page Range Test")
    test_loader = unittest.TestLoader()
    test_names = [
        #'test_bufferpool_write',
        'test_bufferpool_unload'
    ]
    suite = test_loader.loadTestsFromNames(test_names, TestBufferpool)
    runner = unittest.TextTestRunner()
    runner.run(suite)
