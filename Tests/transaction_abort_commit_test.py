from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction

# Open the database and get the "Grades" table.
db = Database()
db.open('./ECS165')
grades_table = db.get_table('Grades')
query = Query(grades_table)

# --- Transaction 1: Insert a new record ---
primary_key = 1001  # use a known primary key for testing
initial_values = [primary_key, 10, 20, 30, 40]  # sample values for all columns

tx_insert = Transaction()
tx_insert.add_query(query.insert, grades_table, *initial_values)
result_insert = tx_insert.run()
if result_insert:
    print("Insert committed successfully.")
else:
    print("Insert aborted.")

# --- Transaction 2: Update record and commit ---
# In this transaction we update one column (say, column index 2) to a new value.
update_values_commit = [None, None, 25, None, None]  # Only update column 2.
tx_update_commit = Transaction()
tx_update_commit.add_query(query.update, grades_table, primary_key, *update_values_commit)
result_update_commit = tx_update_commit.run()
if result_update_commit:
    print("Update (commit) committed successfully.")
else:
    print("Update (commit) aborted.")

# --- Transaction 3: Update record then trigger an abort ---
# We define a dummy query function that always fails.
def failing_query(*args, **kwargs):
    print("This query intentionally fails to trigger an abort.")
    return False

# This transaction first performs an update, then runs a failing query.
update_values_abort = [None, 50, None, None, None]  # update column 1 to new value 50.
tx_update_abort = Transaction()
tx_update_abort.add_query(query.update, grades_table, primary_key, *update_values_abort)
# Add a failing query to force a rollback.
tx_update_abort.add_query(failing_query, grades_table, primary_key)
result_update_abort = tx_update_abort.run()
if result_update_abort:
    print("Update (abort) committed successfully (unexpected).")
else:
    print("Update (abort) aborted successfully.")

# --- Verify final record state ---
# The final state should reflect the committed update (from Transaction 2) and not the aborted update.
result_select = query.select(primary_key, grades_table.key, [1, 1, 1, 1, 1])
if result_select:
    record = result_select[0]
    print("Final record state:", record.columns)
else:
    print("Record not found.")

db.close()
