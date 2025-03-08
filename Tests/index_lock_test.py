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
        seed(3562901)

        def insert_record(thread_id):
            key = 92106429 + thread_id
            record = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

            if not query.table.index.locate(query.table.key, record[0]):
                query.insert(*record)
                records[key] = record
                all_keys.append(key)
                print(f"Thread {thread_id} inserted record: {record}")

        insert_threads = []
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
            updated_columns = [new_key, None, None, None, None]
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
            self.assertEqual(rid, query.table.index.locate(query.table.key, key)[0])
            rid += 1

        def delete_record(thread_id):
            
            key = 92106429 + thread_id + 50 
            query.delete(key)
            print(f"Thread {thread_id} deleted record: {key}")


        delete_threads = []
        for i in range(number_of_records):
            thread = threading.Thread(target=delete_record, args=(i,))
            delete_threads.append(thread)
            thread.start()

        for thread in delete_threads:
            thread.join()

        print("Delete finished")
        print(dict(query.table.index.indices[query.table.key]))

        self.assertEqual(len(query.table.index.indices[query.table.key]), 0)

        

if __name__ == "__main__":
    unittest.main()
