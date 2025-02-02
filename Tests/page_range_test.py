import sys
sys.path.append("..")

from lstore.page import PageRange
from lstore.config import NUM_HIDDEN_COLUMNS

from random import choice, randint, sample, seed

num_records = 1200
num_columns = 3

page_range = PageRange(num_columns)

for i in range(num_records):
    record = []
    for j in range(num_columns + NUM_HIDDEN_COLUMNS):
        record.append(randint(0, 20))

    print(record)

    # prints page index and page slot of the record inserted
    print(page_range.write(record), "\n")