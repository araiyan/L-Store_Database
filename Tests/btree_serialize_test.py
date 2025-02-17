from BTrees.OOBTree import OOBTree
import pickle

btree = OOBTree()

btree[1] = {1:101, 2:2000}
btree[2] = {1:102, 2:2001}

with open("btree_test1.pkl", 'wb') as file:
    pickle.dump(dict(btree), file)

with open("btree_test1.pkl", 'rb') as file:
    print(pickle.load(file))

with open("btree_test2.pkl", 'wb') as file:
    pickle.dump(btree, file)

with open("btree_test2.pkl", 'rb') as file:
    print(dict(pickle.load(file)))