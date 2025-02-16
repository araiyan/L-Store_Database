import sys
sys.path.append("..")

from lstore.index import Index
from lstore.table import Table,Record
from lstore.config import *

import unittest

class TestIndexMethods(unittest.TestCase):
    

    def test_index(self):
            
        table = Table(name="Students", num_columns=4, key=1)

        # Student id, Key, Age, Grade
        record1 = Record(0, table.key, [91900, 101, 20, 90])
        record2 = Record(1, table.key, [91901, 102, 21, 95])
        record3 = Record(2, table.key, [91902, 103, 18, 86])
        record4 = Record(3, table.key, [None, None, 19, 83])
        record5 = Record(4, table.key, [None, None, 20, None])
        record6 = Record(5, table.key, [91903, 104, 19, 99])

           

        index = Index(table)

        # Should return None
        key = index.locate(table.key, 101)
        print(f"Key 101 RID: {key}")

        r1 = [0, record1.rid,0,0,record1.columns[0], record1.columns[1], record1.columns[2], record1.columns[3]]
        r2 = [0, record2.rid,0,0,record2.columns[0], record2.columns[1], record2.columns[2], record2.columns[3]]
        r3 = [0, record3.rid,0,0,record3.columns[0], record3.columns[1], record3.columns[2], record3.columns[3]]

        index_age_to_grade = index.create_index(2,3)
        index.insert_in_all_indices(r1)
        index.insert_in_all_indices(r2)
        index.insert_in_all_indices(r3)

        print(dict(index_age_to_grade))

        # Should raise error for inserting same key more than once
        # index.insert_in_all_indices(*r1)

        rid = index.locate(table.key, 101)
        print(f"Key 101 RID: {rid}")
        self.assertEqual(rid[0], record1.rid)

        rid = index.locate_range(102, 103, table.key)
        print(f"Key 102-103 RID: {rid}")
        self.assertEqual(rid, [record2.rid, record3.rid])

        # Should return None since key 200 doesn't exist
        rid = index.locate(table.key, 200)
        print(f"Key 200 RID: {rid}")

        r4 = [0,record4.rid,0,0, record4.columns[0], record4.columns[1], record4.columns[2], record4.columns[3]]
        index.update_all_indices(103, *r4)
        print(dict(index_age_to_grade))

        # Should raise error since key 200 doesn't exist
        # index.update_all_indices(200, *r4)

        r5 = [0,record5.rid,0,0, record5.columns[0], record5.columns[1], record5.columns[2], record5.columns[3]]
        index.update_all_indices(103, *r5)

        grades = index.get(2,3,20)
        print(f"grades of students age: 20 : {grades}")

        #get something not in secondary index
        age = index.get(3, 2, 83)
        print(f"age of student with grade 83 : {age}")

        print(f"secondary index age to grade: {dict(index_age_to_grade)}")

        column = index.delete_from_all_indices(record2.columns[table.key])
        print(f"deleted column: {column} from all index")
        print(f"secondary index age to grade: {dict(index_age_to_grade)}")

        column = index.delete_from_all_indices(record3.columns[table.key])
        print(f"deleted column: {column} from all index")
        print(f"secondary index age to grade: {dict(index_age_to_grade)}")
        print(dict(index.value_mapper))

        grades = index.get(2,3,20)
        print(f"grades of students age: 20 : {grades}")


        # Test locate after update
        # removed this test bc we're only indexing keys
        # can test this if you uncomment creating all indices in __init__ in index
        #rid = index.locate(2, 20)
        #self.assertEqual(rid, [record1.rid, record5.rid])

        #rid = index.locate(table.key, 103)
        #print(f"Updated again key 103 RID: {rid}")

        #r6 = [0,record6.rid,0,0,record6.columns[0], record6.columns[1], record6.columns[2], record6.columns[3]]
        #index.insert_in_all_indices(*r6)

        #rid = index.locate_range(102, 200, table.key)
        #print(f"Key 102-200 RID: {rid}")



        # Deleting columns with key 102
        #index.delete_from_all_indices(r2[NUM_HIDDEN_COLUMNS + table.key])

        # Should raise error since key 200 doesn't exist
        # index.delete_from_all_indices(200)


unittest.main()