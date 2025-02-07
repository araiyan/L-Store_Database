import sys
sys.path.append("..")

from lstore.index import Index
from lstore.table import Table,Record
from lstore.query import Query
from lstore.config import *

table = Table(name="Students", num_columns=4, key=1)

# student id, key, age, grade
r1 = [91901, 101, 20, 90]
r2 = [91902, 102, 21, 95]
r3 = [91903, 103, 18, 86]
r4 = [91904, 104, 19, 99]

query = Query(table)

query.insert(*r1)
query.insert(*r2)
query.insert(*r3)
query.insert(*r4)

rid = query.table.index.locate(query.table.key, 101)
print(f"Key 101 RID: {rid}")
rid = query.table.index.locate_range(102, 104, query.table.key)
print(f"Key 102-104 RID: {rid}")

print(query.table.page_directory)

for rid, columns in query.table.page_directory.items():
    page_index, page_slot = columns[0]
    column = []
    for i in range(query.table.total_num_columns):
        column.append(query.table.base_pages[i][page_index].get(page_slot))
    print(column)
