INTEGER_BYTE_SIZE = 4

# Page Constants
PAGE_SIZE = 4096
MAX_RECORD_PER_PAGE = PAGE_SIZE // INTEGER_BYTE_SIZE

# Page Range Constants
MAX_PAGE_RANGE = 8
'''How many base pages are in a Page Range'''
MAX_RECORD_PER_PAGE_RANGE = MAX_RECORD_PER_PAGE * MAX_PAGE_RANGE
MAX_TAIL_PAGES_BEFORE_MERGING = 4

# Record Constants
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
UPDATE_TIMESTAMP_COLUMN = 3
SCHEMA_ENCODING_COLUMN = 4

NUM_HIDDEN_COLUMNS = 5

RECORD_DELETION_FLAG = -1
RECORD_NONE_VALUE = -2

# Buffer Pool Constants
MAX_NUM_FRAME = 64
MERGE_FRAME_ALLOCATION = 8