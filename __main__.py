from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

insert_time_0 = process_time()
for i in range(0, 10000):
    query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)
insert_time_1 = process_time()

print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measuring update Performance
update_cols = [
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

update_time_0 = process_time()
for i in range(0, 10000):
    query.update(choice(keys), *(choice(update_cols)))
update_time_1 = process_time()
print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)

# Measuring Select Performance
select_time_0 = process_time()
for i in range(0, 10000):
    query.select(choice(keys),0 , [1, 1, 1, 1, 1])
select_time_1 = process_time()
print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)

# Measuring Aggregate Performance
agg_time_0 = process_time()
for i in range(0, 10000, 100):
    start_value = 906659671 + i
    end_value = start_value + 100
    result = query.sum(start_value, end_value - 1, randrange(0, 5))
agg_time_1 = process_time()
print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)

# Measuring Delete Performance
delete_time_0 = process_time()
for i in range(0, 10000):
    query.delete(906659671 + i)
delete_time_1 = process_time()
print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)

db2 = Database()
bob_table= db2.create_table('bob', 5, 0)
query = Query(bob_table)

# insert initial records
rids = []
for i in range(10):
    pk = 906659671 + i
    query.insert(pk, 90 + i, 0, 0, 0)
    
    # retrieve rids
    rid = bob_table.index.locate(bob_table.key, pk)
    if rid:
        rids.append(rid[0])

print("Inserted Records (Primary Keys):", [906659671 + i for i in range(10)])
print("Inserted Records (RIDs):", rids)

delete_time_0 = process_time()
query.delete(906659671)  # delete first record
query.delete(906659675)  # delete middle record
query.delete(906659679)  # delete last record
delete_time_1 = process_time()
print("Deleting records took:", delete_time_1 - delete_time_0)

# check deallocated
deallocated_rids = list(bob_table.deallocation_base_rid_queue.queue)
print("Deallocated RIDs:", deallocated_rids)

# check allocated
allocated_rids = list(bob_table.allocation_base_rid_queue.queue)
print("Allocated RIDs (Available for Reuse):", allocated_rids)

# insert new records
new_rids = []
for i in range(3):
    pk = 999999990 + i
    query.insert(pk, 95, 85, 75, 65)

    # retrieve assigned rids
    rid = bob_table.index.locate(bob_table.key, pk)
    if rid:
        new_rids.append(rid[0])

print("Inserted New Records (Primary Keys):", [999999990 + i for i in range(3)])
print("Inserted New Records (Actual RIDs):", new_rids)

# final rid list
final_rid_list = list(bob_table.page_directory.keys())
print("Final RID List After Reuse:", final_rid_list)