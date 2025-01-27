"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from lstore.config import *

class Index:

    # Create an index for the Key column using keys and RID from page
    # directory. Keys are mapped to their RID and are inserted together into
    # the BTree
    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns

        self.key_column_num = table.key
        self.key_index= table.page_directory

        self.indices[self.key_column_num] = BTree()
        for item in self.key_index.items(): 

            self.indices[self.key_column_num].insert(item)


    """
    # returns the location of all records with the given value on column "column"
    """
    # Currently using column number to search up the values and their RID.
    # We could adjust replace column number with column name when we come 
    # up with column names.

    def locate(self, column, value):
        result = self.indices[column].search_all(value,value)
        return result
            

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    # Similarly using column number for searches instead of column name
    def locate_range(self, begin, end, column):
        result = self.indices[column].search_all(begin, end)
        return result    

    """
    # optional: Create index on specific column
    """
    # Unfinished

    def create_index(self, column_number, *column):
        """Create an index for a specific column."""
        if self.indices[column_number] is None:
            hash_table = {}
            # Insert column values into hash table after getting columns
            # from actual page

            self.indices[column_number] = BTree()
            for item in hash_table.items(): 
                self.indices[column_number].insert(item)

    """
    # optional: Drop index of specific column
    """
    # Drop the reference to the index
    def drop_index(self, column_number):
        """Drop the index for a specific column."""
        self.indices[column_number] = None

# BTree
# https://www.geeksforgeeks.org/b-tree-in-python/
# https://www.geeksforgeeks.org/introduction-of-b-tree-2/

class BTreeNode:
    def __init__(self, leaf=True):
        self.leaf = leaf
        self.keys = []
        self.children = []

class BTree:
    def __init__(self):
        self.root = BTreeNode(True)

    def insert(self, value):
        num_child_nodes = 3
        root = self.root

        if len(root.keys) == (2 * num_child_nodes) - 1:
            temp_root = BTreeNode(leaf=False)
            self.root = temp_root
            temp_root.children.append(root)
            self.split_child(temp_root, 0, num_child_nodes)
            self.insert_non_full(temp_root, value, num_child_nodes)
        else:
            self.insert_non_full(root, value, num_child_nodes)

    def insert_non_full(self, cur_node, cur_key, num_child_nodes):
        i = len(cur_node.keys) - 1

        if cur_node.leaf:
            cur_node.keys.append(None)

            while i >= 0 and cur_key < cur_node.keys[i]:
                cur_node.keys[i + 1] = cur_node.keys[i]
                i -= 1
            cur_node.keys[i + 1] = cur_key
        else:
            while i >= 0 and cur_key < cur_node.keys[i]:
                i -= 1
            i += 1
            if len(cur_node.children[i].keys) == (2 * num_child_nodes) - 1:
                self.split_child(cur_node, i, num_child_nodes)
                if cur_key > cur_node.keys[i]:
                    i += 1

            self.insert_non_full(cur_node.children[i], cur_key, num_child_nodes)

    def split_child(self, cur_node, i, num_child_nodes):
        prev_child = cur_node.children[i]
        new_child = BTreeNode(leaf=prev_child.leaf)

        cur_node.keys.insert(i, prev_child.keys[num_child_nodes - 1])
        new_child.keys = prev_child.keys[num_child_nodes: (2 * num_child_nodes) - 1]
        prev_child.keys = prev_child.keys[0: num_child_nodes - 1]

        if not prev_child.leaf:
            new_child.children = prev_child.children[num_child_nodes: 2 * num_child_nodes]
            prev_child.children = prev_child.children[0: num_child_nodes - 1]

        cur_node.children.insert(i + 1, new_child)
    
    def search_all(self, begin, end, node=None):
        if node is None:
            node = self.root

        all_values = []
        # RID to value are mapped as a tuple
        rid = 0
        value = 1
        for i in range(len(node.keys)):
            if repr(begin) <= repr(node.keys[i][value]) <= repr(end):
                all_values.append(node.keys[i][rid])

        if node.leaf:
            if all_values:
                return all_values
            else:
                return None

        for child in node.children:
            result = self.search_all(begin, end, child)
            if result:
                all_values.extend(result)
                
        return all_values 