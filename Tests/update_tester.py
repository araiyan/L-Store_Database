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

# Define updates:
# We'll update column index 2 (third column) and column index 3 (fourth column)
updated_columns = [None, None, None, None, None]
updated_columns[2] = 20  # Update grade at column 2 to 20
updated_columns[3] = 19  # Update grade at column 3 to 19

# Update our test record copy for later comparison
records[92106429][2] = 20
records[92106429][3] = 19

# Execute the update on the record with student_id 92106429
query.update(92106429, *updated_columns)

# Retrieve the updated record from the table.
# The select function returns a list of
