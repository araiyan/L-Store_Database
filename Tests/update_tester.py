import sys
import random

sys.path.append("..")
from lstore.db import Database
from lstore.query import Query

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

num_records = 10
records = {}
student_ids = random.sample(range(10000000, 10010000), num_records)

print("Inserting records:")
for student_id in student_ids:
    record = [student_id] + [random.randint(10, 20) for _ in range(4)]
    query.insert(*record)
    records[student_id] = record.copy()
    print(f"  Student {student_id}:", record)

num_updates = 3
for student_id in sorted(records.keys()):
    print(f"\nPerforming updates for student {student_id}:")
    for update_num in range(num_updates):
        updated_columns = [None] * 5
        num_cols_to_update = random.randint(1, 4)
        cols_to_update = random.sample(range(1, 5), num_cols_to_update)
        for col in cols_to_update:
            new_val = random.randint(10, 30)
            updated_columns[col] = new_val
            records[student_id][col] = new_val
        print(f"  Update #{update_num + 1} with:", updated_columns)
        query.update(student_id, *updated_columns)
        record_after = query.select(student_id, 0, [1, 1, 1, 1, 1])[0]
        if record_after.columns == records[student_id]:
            print(f"    Update #{update_num + 1} successful:", record_after.columns)
        else:
            print(f"    Update #{update_num + 1} error: expected", records[student_id], "but got", record_after.columns)

print("\nFinal verification of all records:")
for student_id in sorted(records.keys()):
    record_after = query.select(student_id, 0, [1, 1, 1, 1, 1])[0]
    print(f"  Student {student_id}: {record_after.columns}")
