import sys
sys.path.append("..")
import unittest
import threading
from random import randint, seed
from lstore.db import Database
from lstore.query import Query

class TestIndexLocks(unittest.TestCase):

    def test_update(self):
        db = Database()
        db.open('./ECS165')
        grades_table = db.create_table('Grades', 5, 0)
        query = Query(grades_table)

        records = {}
        all_keys = []

        number_of_records = 10
        new_index = 2
        seed(3562901)

        def create_index_test(thread_id, column_number):
            query.table.index.create_index(column_number)
            print(f"Thread {thread_id} created index on column: {column_number}")
        
        def drop_index_test(thread_id, column_number):
            query.table.index.drop_index(column_number)
            print(f"Thread {thread_id} dropped index on column: {column_number}")

        def insert_record(thread_id):
            key = 92106429 + thread_id
            record = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
            query.insert(*record)
            records[key] = record
            all_keys.append(key)
            print(f"Thread {thread_id} inserted record: {record}")


        insert_threads = []

        create_thread = threading.Thread(target=create_index_test, args=(11, new_index))
        insert_threads.append(create_thread)
        create_thread.start()

        for i in range(number_of_records):
            thread = threading.Thread(target=insert_record, args=(i,))
            insert_threads.append(thread)
            thread.start()

        for thread in insert_threads:
            thread.join()

        print("Insert finished")

        self.assertEqual(len(records), number_of_records)
        self.assertEqual(len(set(query.table.index.locate_range(92106429, 92106438, query.table.key))), number_of_records)
        self.assertEqual(query.table.index.locate_range(92106429, 92106438, query.table.key), list(range(0, 10)))


        update_records = {}

        def update_record(thread_id):
            key = 92106429 + thread_id
            new_key = 92106429 + thread_id + 50
            updated_columns = [new_key, None, 200 + thread_id, 300 + thread_id, None]
            update_records[new_key] = updated_columns 

            query.update(key, *updated_columns)
            print(f"Thread {thread_id} updated record: {key} to {new_key}")

        update_threads = []
        for i in range(number_of_records):
            thread = threading.Thread(target=update_record, args=(i,))
            update_threads.append(thread)
            thread.start()

        for thread in update_threads:
            thread.join()

        print("Update finished")

        rid = 0

        for key, value in update_records.items():
            self.assertEqual(rid, query.table.index.locate(query.table.key, key)[0], key)
            rid += 1


        db.close()
        db.open('./ECS165')
        

        # Getting the existing Grades table
        grades_table = db.get_table('Grades')

        query = Query(grades_table)

        def delete_record(thread_id):
            
            key = 92106429 + thread_id + 50 
            query.delete(key)
            print(f"Thread {thread_id} deleted record: {key}")


        delete_threads = []

        drop_thread = threading.Thread(target=drop_index_test, args=(11, new_index))
        delete_threads.append(drop_thread)
        drop_thread.start()

        for i in range(number_of_records):
            thread = threading.Thread(target=delete_record, args=(i,))
            delete_threads.append(thread)
            thread.start()

        for thread in delete_threads:
            thread.join()

        print("Delete finished")

        self.assertEqual(len(query.table.index.indices[query.table.key]), 0)
        self.assertEqual(query.table.index.indices[new_index], None)


        

if __name__ == "__main__":
    unittest.main()
