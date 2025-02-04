import sys
sys.path.append("..")

from lstore.index import Index
from lstore.table import Table,Record
from lstore.config import *


table = Table(name="Students", num_columns=5, key=1)

# RID, Key, Name, Age, Grade
record1 = Record(1, 101, ["Alice", 20, 90])
record2 = Record(2, 102, ["Bob", 21, 95])
record3 = Record(3, 103, ["Charlie", 18, 86])
record4 = Record(4, 103, ["Charlie", 19, 83])
record5 = Record(5, 103, ["Charlie", 20, 70])
record6 = Record(6, 104, ["Dylan", 19, 99])

index = Index(table)

# Should return None
key = index.locate(table.key, 101)
print(f"Key 101 RID: {key}")

r1 = [0,1,0,0,record1.rid, record1.key, record1.columns[0], record1.columns[1], record1.columns[2]]
r2 = [0,2,0,0,record2.rid, record2.key, record2.columns[0], record2.columns[1], record2.columns[2]]
r3 = [0,3,0,0,record3.rid, record3.key, record3.columns[0], record3.columns[1], record3.columns[2]]

index.insert_in_all_indices(*r1)
index.insert_in_all_indices(*r2)
index.insert_in_all_indices(*r3)

# Should raise error for inserting same key more than once
# index.insert_in_all_indices(*r1)

rid = index.locate(2, "Alice")
print(f"Value Alice RID: {rid}")

rid = index.locate(table.key, 101)
print(f"Key 101 RID: {rid}")
rid = index.locate_range(102, 103, table.key)
print(f"Key 102-103 RID: {rid}")

rid = index.locate_range("Alice", "Charlie", 2)
print(f"Alice to Charlie RID: {rid}")

# Should return None since key 200 doesn't exist
rid = index.locate(table.key, 200)
print(f"Key 200 RID: {rid}")

r4 = [0,4,0,0,record4.rid, record4.key, record4.columns[0], record4.columns[1], record4.columns[2]]
index.update_all_indices(r4[NUM_HIDDEN_COLUMNS + table.key], *r4)

# Should raise error since key 200 doesn't exist
# index.update_all_indices(200, *r4)

rid = index.locate(table.key, 103)
print(f"Updated key 103 RID: {rid}")

r5 = [0,5,0,0,record5.rid, record5.key, record5.columns[0], record5.columns[1], record5.columns[2]]
index.update_all_indices(r5[NUM_HIDDEN_COLUMNS + table.key], *r5)

rid = index.locate(table.key, 103)
print(f"Updated again key 103 RID: {rid}")

rid = index.locate_range("Alice", "Charlie", 2)
print(f"Updated Alice to Charlie RID: {rid}")

r6 = [0,6,0,0,record6.rid, record6.key, record6.columns[0], record6.columns[1], record6.columns[2]]
index.insert_in_all_indices(*r6)

rid = index.locate_range(102, 200, table.key)
print(f"Key 102-200 RID: {rid}")

# Deleting columns with key 102
index.delete_from_all_indices(r2[NUM_HIDDEN_COLUMNS + table.key])

# Should raise error since key 200 doesn't exist
# index.delete_from_all_indices(200)

# Prints column values in 2 tuple (rid, value)
for index in index.indices:
    print(index)