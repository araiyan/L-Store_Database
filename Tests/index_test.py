import sys
sys.path.append("..")
from random import choice, randint, sample, seed

from lstore.index import Index
from lstore.table import Table,Record
from lstore.config import *
from lstore.db import Database
from lstore.query import Query

import unittest

class TestIndexMethods(unittest.TestCase):
    

    def test_index(self):

        
        db = Database()
        db.open('./ECS165')

        grades_table = db.create_table('Grades', 5, 0)


        query = Query(grades_table)


        records = {}

        number_of_records = 1000
        number_of_aggregates = 100
        number_of_updates = 1

        seed(3562901)
        all_keys = []
        for i in range(0, number_of_records):
            key = 92106429 + i

            records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

            if key == 92106430:
                print(records[key])

            all_keys.append(key)
            query.insert(*records[key])
        keys = sorted(list(records.keys()))
        print("Insert finished")

        #create index on column 1
        grades_table.index.create_index(1)

        #locate specific record and get rid
        rid  = query.table.index.locate(query.table.key, 92106430)
        print(rid)
        #rid = 1, column value at column 1 = 19

        #print what index look like before update
        print(dict(grades_table.index.indices[1][1]))
        print(dict(grades_table.index.indices[1][19]))

        #update column value from 19 to 1 and check if index is updated
        query.update(92106430, *[None, 1, None,None,None])
        print(query.table.index.locate(1, 1))
        #print(dict(grades_table.index.indices[1][19]))
        print(query.table.index.locate(1, 19))
        #print(dict(grades_table.index.indices[1][1]))
        #print(dict(grades_table.index.indices[1][19]))

        #update to unique value
        query.update(92106430, *[None, 123456789, 999999,None,None])
        print(query.table.index.locate(1, 123456789))
        print(query.table.index.locate(1, 1))

        
        #test delete
        #query.table.index.update_all_indices(92106430, [0,0,0,0,0, 0, None, 1, None,None,None])
        print(dict(grades_table.index.indices[1][123456789]))
        query.table.index.delete_from_all_indices(92106430)

        grades_table.index.create_index(2)
        print(f"create secondary index after updates: {query.table.index.locate(2, 999999)}")
        print(f"create secondary index after updates: {query.table.index.locate(2,1)}")
        print(f"create secondary index after updates: {query.table.index.locate(2,2)}")
        print(f"create secondary index after updates: {query.table.index.locate_range(1, 20,2)}")

        # throws error bc its already deleted
        #print(dict(grades_table.index.indices[1][123456789]))
        

        #grades_table.index.serialize_all_indices()
        #grades_table.index.deserialize_all_indices()

        #for item in all_keys:
        #    print(grades_table.index.locate(grades_table.key, item))
        #    print(grades_table.index.value_mapper[item])
            


unittest.main()