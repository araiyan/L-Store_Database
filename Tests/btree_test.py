from BTrees.OOBTree import OOBTree

tree = OOBTree()

tree[1] = 1235
tree[2] = 1236

print(list(tree.values(min=1, max=2)))

dicTree = OOBTree()

dicTree[1] = {1: 1235, 2: 1236, 6: 1239}
dicTree[2] = {4: 1237, 5: 1238}

print(list(dicTree.values(min=1, max=5)))

del dicTree[1][1]

print(list(dicTree.values(min=1, max=5)))

ke1 = dicTree.pop(1)
ke1.update({3: 1239})
dicTree.insert(1.3124, ke1)

# print(list(dicTree.get(1, None)))
print(list(dicTree.get(2, [])) or None)

