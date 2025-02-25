import sys
sys.path.append("..")
from lstore.db import Database
from lstore.query import Query

# Create the database and the Grades table with 5 columns
# The first column is the student ID (which is the primary key)
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

# Insert a hardcoded record into the table:
# Format: [student_id, grade1, grade2, grade3, grade4]
record = [92106429, 15, 18, 17, 16]
query.insert(*record)

# For verification, we keep a copy of the record in a dictionary
records = {92106429: record.copy()}

print("Original record:", record)

# First Update
updated_columns = [None, None, None, None, None]
updated_columns[2] = 20  # Update grade at column 2 to 20
updated_columns[3] = 19  # Update grade at column 3 to 19

# Update our test record copy for later comparison
records[92106429][2] = 20
records[92106429][3] = 19

# Execute the first update
print("\nExecuting first update with:", updated_columns)
record = query.update(92106429, *updated_columns)

# Verify first update
record_after = query.select(92106429, 0, [1, 1, 1, 1, 1])[0]
if record_after.columns == records[92106429]:
    print("First update successful:", record_after.columns)
else:
    print("First update error: expected", records[92106429], "but got", record_after.columns)

# Second Update
updated_columns_2 = [None, None, None, None, None]
updated_columns_2[1] = 25  # Update grade at column 1 to 25
updated_columns_2[4] = 22  # Update grade at column 4 to 22

# Update our test record copy for the second update
records[92106429][1] = 25
records[92106429][4] = 22

# Execute the second update
print("\nExecuting second update with:", updated_columns_2)
record = query.update(92106429, *updated_columns_2)

# Verify second update
record_after = query.select(92106429, 0, [1, 1, 1, 1, 1])[0]
if record_after.columns == records[92106429]:
    print("Second update successful:", record_after.columns)
else:
    print("Second update error: expected", records[92106429], "but got", record_after.columns)

# Final record state should be:
# [92106429, 25, 20, 19, 22]
