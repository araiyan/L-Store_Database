import sys
sys.path.append("..")
import unittest
from lstore.db import Database
from lstore.query import Query
from random import randint, seed
from random import choice, randint, sample, seed

class TestQuery(unittest.TestCase):

    def setUp(self):
        """
        Setup the Database and Table for testing
        """
        self.db = Database()
        # Create a table  with 5 columns: Student Id and 4 grades
        # The first column is the primary key
        self.table = self.db.create_table('Grades', 5, 0)
        self.query = Query(self.table)
        self.records = {}

        # Insert sample records
        seed(42)  # Seed for reproducibility
        for i in range(10):
            key = 1000 + i
            self.records[key] = [key, randint(0, 100), randint(0, 100), randint(0, 100), randint(0, 100)]
            self.query.insert(*self.records[key])

    def test_sum(self):
        """
        Test the sum function for a range of primary keys and a specific column
        """
        # Test sum of column 1 for the first 5 records
        start_range = 1000
        end_range = 1004
        column_index = 1
        expected_sum = sum(self.records[key][column_index] for key in range(start_range, end_range+1))
        
        result = self.query.sum(start_range, end_range, column_index)
        self.assertEqual(result, expected_sum, f"Expected sum: {expected_sum}, but got {result}")
        
        print("query.sum test completed successfully for expected sum:", expected_sum)       

    def test_sum_version(self):
        """
        Test the sum_version function for a range of primary keys and a specific column version
        """
        start_range = 1000
        end_range = 1004
        column_index = 1
        relative_version = 0
        expected_sum = sum(self.records[key][column_index] for key in range(start_range, end_range+1))
        
        result = self.query.sum_version(start_range, end_range, column_index, relative_version)
        self.assertEqual(result, expected_sum, f"Expected sum: {expected_sum}, but got {result}")
        
        print("query.sum_version test completed successfully for expected sum:", expected_sum)   

    def test_select(self):
        """
        Test the select function for a specific primary key and all columns
        """
        key = 1002
        projected_columns = [1, 1, 1, 1, 1]  # Select all columns
        
        record = self.query.select(key, 0, projected_columns)[0]
        self.assertIsNotNone(record, f"Select returned None for key {key}")
        
        expected_record = self.records[key]
        for i, column in enumerate(record.columns):
            self.assertEqual(column, expected_record[i], f"Mismatch at column {i}: expected {expected_record[i]}, but got {column}")
            print("query.select test completed successfully for expected expected_record[i]:", expected_record[i])   

    def test_select_version(self):
        """
        Test the select_version function for a specific primary key and version
        """
        key = 1002
        projected_columns = [1, 1, 1, 1, 1]  # Select all columns
        relative_version = 0
        
        record = self.query.select_version(key, 0, projected_columns, relative_version)[0]
        self.assertIsNotNone(record, f"Select version returned None for key {key}")
        
        expected_record = self.records[key]
        for i, column in enumerate(record.columns):
            self.assertEqual(column, expected_record[i], f"Mismatch at column {i}: expected {expected_record[i]}, but got {column}")
            print("query.select_version test completed successfully for expected_record[i]:", expected_record[i])   

    def test_sum_empty_range(self):
        """
        Test the sum function for a range that has no records
        """
        start_range = 999
        end_range = 999
        column_index = 1
        
        result = self.query.sum(start_range, end_range, column_index)
        self.assertFalse(result, f"Expected False for empty range, but got {result}")
        print("query.sum test completed successfully for empty range")   
      
    def test_sum_version_empty_range(self):
        """
        Test the sum_version function for a range that has no records
        """
        start_range = 999
        end_range = 999
        column_index = 1
        relative_version = 0
        
        result = self.query.sum_version(start_range, end_range, column_index, relative_version)
        self.assertFalse(result, f"Expected False for empty range, but got {result}")
        print("query.sum_version test completed successfully for empty range")   

    def test_select_non_existent_key(self):
        """
        Test the select function with a key that doesn't exist
        """
        key = 999
        projected_columns = [1, 1, 1, 1, 1]  # Select all columns
        
        with self.assertRaises(ValueError):
            self.query.select(key, 0, projected_columns)
        
        print("query.select test completed successfully for non existenct key")   

    def test_select_version_non_existent_key(self):
        """
        Test the select_version function with a key that doesn't exist
        """
        key = 999
        projected_columns = [1, 1, 1, 1, 1]  # Select all columns
        relative_version = 0
        
        with self.assertRaises(ValueError):
            self.query.select_version(key, 0, projected_columns, relative_version)
        print("query.select_version test completed successfully for non existent key")   

    def tearDown(self):
        """
        Clean up the table and database after each test
        """
        self.db.drop_table('Grades')


if __name__ == '__main__':
    unittest.main()