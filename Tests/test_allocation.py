import sys
sys.path.append("..")
import unittest
from lstore.db import Database
from lstore.query import Query
from time import process_time


class TestAllocationDeallocation(unittest.TestCase):
    '''Test to ensure the validity of __delete_worker'''

    def test(self):
      db2 = Database()
      bob_table= db2.create_table('bob', 5, 0)
      query = Query(bob_table)

      # insert initial records
      rids = []
      for i in range(10):
        pk = 906659671 + i
        query.insert(pk, 90 + i, 0, 0, 0)
        
        # retrieve rids
        rid = bob_table.index.locate(bob_table.key, pk)
        if rid:
            rids.append(rid[0])

      print("Inserted Records (Primary Keys):", [906659671 + i for i in range(10)])
      print("Inserted Records (RIDs):", rids)

      delete_time_0 = process_time()
      query.delete(906659671)  # delete first record
      query.delete(906659675)  # delete middle record
      query.delete(906659679)  # delete last record
      delete_time_1 = process_time()
      print("Deleting records took:", delete_time_1 - delete_time_0)

      # check deallocated
      deallocated_rids = list(bob_table.deallocation_base_rid_queue.queue)
      print("Deallocated RIDs:", deallocated_rids)

      # check allocated
      allocated_rids = list(bob_table.allocation_base_rid_queue.queue)
      print("Allocated RIDs (Available for Reuse):", allocated_rids)

      # insert new records
      new_rids = []
      for i in range(3):
        pk = 999999990 + i
        query.insert(pk, 95, 85, 75, 65)

        # retrieve assigned rids
        rid = bob_table.index.locate(bob_table.key, pk)
        if rid:
            new_rids.append(rid[0])

      print("Inserted New Records (Primary Keys):", [999999990 + i for i in range(3)])
      print("Inserted New Records (Actual RIDs):", new_rids)

      # final rid list
      final_rid_list = list(bob_table.page_directory.keys())
      print("Final RID List After Reuse:", final_rid_list)
    
if __name__ == "__main__":
    unittest.main()
