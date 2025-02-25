import sys
sys.path.append("..")
import json

from lstore.page_range import PageRange
from lstore.table import Table


def test_serialize_deserialize():
    # 1. Create a Table object with sample data
    table = Table(name="Employee", num_columns=3, key=0, db_path="/tmp/db")

    # Simulate sample data in Page Directory and Page Ranges
    table.page_directory = {
        101: (0, 2, 3),
        102: (1, 0, 1)
    }
    table.rid_index = 103  # Simulating next available RID

    # Fix: Initialize PageRange with required arguments
    page_range1 = PageRange(page_range_index=0, num_columns=table.num_columns, bufferpool=table.bufferpool)
    page_range2 = PageRange(page_range_index=1, num_columns=table.num_columns, bufferpool=table.bufferpool)
    table.page_ranges = [page_range1, page_range2]

    # Mock serialize and deserialize methods for PageRange
    page_range1.serialize = lambda: {"id": 1, "pages": ["PageA", "PageB"]}
    page_range2.serialize = lambda: {"id": 2, "pages": ["PageC", "PageD"]}
    page_range1.deserialize = lambda data: None
    page_range2.deserialize = lambda data: None

    # Mock Index serialization and deserialization
    table.index.indices = [
        {1: {101, 102}, 2: {103}},   # Index for first column
        None,                       # No index on the second column
        {3: {101, 103}, 4: {102}}    # Index for third column
    ]
    table.index.value_mapper = [None, None, None]
    table.index.num_columns = table.num_columns
    table.index.key = table.key
    
    # Mocking the serialize method for Index
    table.index.serialize = lambda: {
        "indices": [
            {1: [101, 102], 2: [103]},   # Serialized index for first column
            None,                       # No index for second column
            {3: [101, 103], 4: [102]}    # Serialized index for third column
        ],
        "value_mapper": [None, None, None],
        "num_columns": table.num_columns,
        "key": table.key
    }
    
    # Mocking the deserialize method for Index
    table.index.deserialize = lambda data: None

    # 2. Serialize the Table
    serialized_data = table.serialize()
    print("Serialized Data:", json.dumps(serialized_data, indent=4))

    # 3. Serialize to JSON string for storage simulation
    json_data = json.dumps(serialized_data)

    # 4. Deserialize the JSON back to a new Table object
    new_table = Table(name="Employee", num_columns=3, key=0, db_path="/tmp/db")
    new_table.deserialize(json.loads(json_data))

    # 5. Assert that original and deserialized data are identical
    assert table.name == new_table.name, "Table name mismatch"
    assert table.num_columns == new_table.num_columns, "Number of columns mismatch"
    assert table.key == new_table.key, "Primary key index mismatch"
    assert table.rid_index == new_table.rid_index, "RID index mismatch"
    assert table.page_directory == new_table.page_directory, "Page Directory mismatch"
    assert len(table.page_ranges) == len(new_table.page_ranges), "Page Range count mismatch"

    print("Test passed! Serialize and Deserialize are working correctly.")

# Run the test
test_serialize_deserialize()