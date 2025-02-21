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
        record3 = Record(2, table.key, [91902, 103, 18, 90])
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

        index.insert_in_all_indices(r1)
        index.insert_in_all_indices(r2)
        index.insert_in_all_indices(r3)

        all_rid = index.grab_all()
        print(f"all_rid: {all_rid}")

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
        index.update_all_indices(103, r4)

        r5 = [0,record5.rid,0,0, record5.columns[0], record5.columns[1], record5.columns[2], record5.columns[3]]
        index.update_all_indices(103, r5)

        index.create_index(3, table.key)
        rid_of_grades = index.locate(3,90)
        print(f"rid of students with grade 90: {rid_of_grades}")

        rid_of_grades = index.locate_range(50,92,3)
        print(f"rid of students with grade 50-92: {rid_of_grades}")
        
        print("get stuff from value mapper")
        grades = index.get(table.key,3,101)
        print(f"grades of students with primary key 101 : {grades}")

        grades = index.get_range(table.key,3,101,103)
        print(f"grades of students with primary key 101-103 : {grades}")

        print("create and get stuff from secondary index")
        index_primary_key_to_grade = index.create_index(table.key,3)
        #print(dict(index_primary_key_to_grade))

        grades = index.get(table.key,3,101)
        print(f"grades of students with primary key 101 : {grades}")

        grades = index.get_range(table.key,3,101,103)
        print(f"grades of students with primary key 101-103 : {grades}")


        index_age_to_grade = index.create_index(2,3)
        print(dict(index_age_to_grade))

        grades = index.get(2,3,20)
        print(f"grades of students age: 20 : {grades}")

        grades = index.get_range(2,3,20,50)
        print(f"grades of students age: 20-50 : {grades}")

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


unittest.main()