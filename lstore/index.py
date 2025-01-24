"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    # Create an index for the Key column using keys and RID from the array 
    # of records. Keys are mapped to their RID and are inserted together into
    # the BTree
    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns

        self.column_num_for_key = table.key
        self.key_index= {}
        self.records = table.records

        for record in self.records:
            self.key_index[record.rid] = [record.key]

        self.indices[self.column_num_for_key] = BTree(3)
        for item in self.key_index.items(): 
            self.indices[self.column_num_for_key].insert(item)


    """
    # returns the location of all records with the given value on column "column"
    """
    # Currently using column number to search up the values and their RID.
    # We could adjust replace column number with column name when we come 
    # up with column names.

    #Referencing from the Record class, RID is column 0 and Key is column 1
    def locate(self, column, value):
        result = self.indices[column].search(value)
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
    # Create a hash table to map value in column number to its rid
    # and then insert each item in the hash table into a B tree.
    # Reference the column number'th value in the array of indices to
    # the Btree

    def create_index(self, column_number):
        """Create an index for a specific column."""
        if self.indices[column_number] is None:
            hash_table = {}
            for record in self.records:
                hash_table[record.rid] = [record.columns[column_number-2]]

            self.indices[column_number] = BTree(3)
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
    def __init__(self, t):
        self.root = BTreeNode(True)
        self.t = t

    def insert(self, k):
        root = self.root
        if len(root.keys) == (2 * self.t) - 1:
            temp = BTreeNode(leaf=False)
            self.root = temp
            temp.children.append(root)
            self.split_child(temp, 0)
            self.insert_non_full(temp, k)
        else:
            self.insert_non_full(root, k)

    def insert_non_full(self, x, k):
        i = len(x.keys) - 1
        if x.leaf:
            x.keys.append(None)
            while i >= 0 and k < x.keys[i]:
                x.keys[i + 1] = x.keys[i]
                i -= 1
            x.keys[i + 1] = k
        else:
            while i >= 0 and k < x.keys[i]:
                i -= 1
            i += 1
            if len(x.children[i].keys) == (2 * self.t) - 1:
                self.split_child(x, i)
                if k > x.keys[i]:
                    i += 1
            self.insert_non_full(x.children[i], k)

    def split_child(self, x, i):
        t = self.t
        y = x.children[i]
        z = BTreeNode(leaf=y.leaf)
        x.keys.insert(i, y.keys[t - 1])
        z.keys = y.keys[t: (2 * t) - 1]
        y.keys = y.keys[0: t - 1]
        if not y.leaf:
            z.children = y.children[t: 2 * t]
            y.children = y.children[0: t - 1]
        x.children.insert(i + 1, z)

    def search(self, value, node=None):
        node = self.root
        all_values = []
        i = 0
        for i in range(len(node.keys)):
            if value == node.keys[i][1][0]:
                all_values.append(node.keys[i][0])

        if node.leaf:
            if all_values:
                return all_values
            else:
                return None
        
        for child in node.children:
            result = self.search_all(value, child)
            if result:
                all_values.extend(result)


    
    def search_all(self, begin, end,node=None):
        node = self.root
        all_values = []
        i = 0
        for i in range(len(node.keys)):
            if begin <= int(node.keys[i][1][0]) <= end:
                all_values.append(node.keys[i][0])

        if node.leaf:
            if all_values:
                return all_values
            else:
                return None
        
        for child in node.children:
            result = self.search_all(begin, end, child)
            if result:
                all_values.extend(result)